/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { BadRequestException, Injectable, Logger, NotFoundException } from '@nestjs/common'
import { OnEvent } from '@nestjs/event-emitter'
import { InjectRepository } from '@nestjs/typeorm'
import { InjectRedis } from '@nestjs-modules/ioredis'
import { Redis } from 'ioredis'
import { In, Not, Repository } from 'typeorm'
import { SANDBOX_STATES_CONSUMING_COMPUTE } from '../constants/sandbox-states-consuming-compute.constant'
import { SANDBOX_STATES_CONSUMING_DISK } from '../constants/sandbox-states-consuming-disk.constant'
import { SNAPSHOT_USAGE_IGNORED_STATES } from '../constants/snapshot-usage-ignored-states.constant'
import { VOLUME_USAGE_IGNORED_STATES } from '../constants/volume-usage-ignored-states.constant'
import { OrganizationUsageOverviewDto } from '../dto/organization-usage-overview.dto'
import {
  SandboxPendingUsageOverviewInternalDto,
  SandboxUsageOverviewInternalDto,
  SandboxUsageOverviewWithPendingInternalDto,
} from '../dto/sandbox-usage-overview-internal.dto'
import { SnapshotUsageOverviewInternalDto } from '../dto/snapshot-usage-overview-internal.dto'
import { VolumeUsageOverviewInternalDto } from '../dto/volume-usage-overview-internal.dto'
import { Organization } from '../entities/organization.entity'
import {
  getResourceTypeFromQuota,
  OrganizationUsageQuotaType,
  OrganizationUsageResourceType,
} from '../helpers/organization-usage.helper'
import { RedisLockProvider } from '../../sandbox/common/redis-lock.provider'
import { SandboxEvents } from '../../sandbox/constants/sandbox-events.constants'
import { SnapshotEvents } from '../../sandbox/constants/snapshot-events'
import { VolumeEvents } from '../../sandbox/constants/volume-events'
import { Sandbox } from '../../sandbox/entities/sandbox.entity'
import { Snapshot } from '../../sandbox/entities/snapshot.entity'
import { Volume } from '../../sandbox/entities/volume.entity'
import { SandboxCreatedEvent } from '../../sandbox/events/sandbox-create.event'
import { SandboxStateUpdatedEvent } from '../../sandbox/events/sandbox-state-updated.event'
import { SnapshotCreatedEvent } from '../../sandbox/events/snapshot-created.event'
import { SnapshotStateUpdatedEvent } from '../../sandbox/events/snapshot-state-updated.event'
import { VolumeCreatedEvent } from '../../sandbox/events/volume-created.event'
import { VolumeStateUpdatedEvent } from '../../sandbox/events/volume-state-updated.event'

@Injectable()
export class OrganizationUsageService {
  private readonly logger = new Logger(OrganizationUsageService.name)

  /**
   * Time-to-live for cached quota usage values
   */
  private readonly CACHE_TTL_SECONDS = 60

  /**
   * Cache is considered stale if it was last populated from db more than CACHE_MAX_AGE_MS ago
   */
  private readonly CACHE_MAX_AGE_MS = 60 * 60 * 1000

  constructor(
    @InjectRedis() private readonly redis: Redis,
    @InjectRepository(Organization)
    private readonly organizationRepository: Repository<Organization>,
    @InjectRepository(Sandbox)
    private readonly sandboxRepository: Repository<Sandbox>,
    @InjectRepository(Snapshot)
    private readonly snapshotRepository: Repository<Snapshot>,
    @InjectRepository(Volume)
    private readonly volumeRepository: Repository<Volume>,
    private readonly redisLockProvider: RedisLockProvider,
  ) {}

  // ===== PUBLIC METHODS FOR GETTING TOTAL/SANDBOX/SNAPSHOT/VOLUME USAGE OVERVIEWS =====

  /**
   * Get the usage overview for all quotas of an organization.
   *
   * @param organizationId
   * @param organization - Provide the organization entity to avoid fetching it from the database (optional)
   */
  async getUsageOverview(organizationId: string, organization?: Organization): Promise<OrganizationUsageOverviewDto> {
    if (organization && organization.id !== organizationId) {
      throw new BadRequestException('Organization ID mismatch')
    }

    if (!organization) {
      organization = await this.organizationRepository.findOne({ where: { id: organizationId } })
    }

    if (!organization) {
      throw new NotFoundException(`Organization with ID ${organizationId} not found`)
    }

    const sandboxUsageOverview = await this.getSandboxUsageOverview(organizationId)
    const snapshotUsageOverview = await this.getSnapshotUsageOverview(organizationId)
    const volumeUsageOverview = await this.getVolumeUsageOverview(organizationId)

    return {
      totalCpuQuota: organization.totalCpuQuota,
      totalMemoryQuota: organization.totalMemoryQuota,
      totalDiskQuota: organization.totalDiskQuota,
      totalSnapshotQuota: organization.snapshotQuota,
      totalVolumeQuota: organization.volumeQuota,
      ...sandboxUsageOverview,
      ...snapshotUsageOverview,
      ...volumeUsageOverview,
    }
  }

  /**
   * Get the usage overview for all sandbox quotas of an organization.
   *
   * @param organizationId
   * @param excludeSandboxId - If provided, the usage overview will be returned without the usage of the sandbox with the given ID
   */
  async getSandboxUsageOverview(
    organizationId: string,
    excludeSandboxId?: string,
  ): Promise<SandboxUsageOverviewInternalDto> {
    let cachedUsageOverview = await this.getCachedSandboxUsageOverview(organizationId)

    // cache hit
    if (cachedUsageOverview) {
      if (excludeSandboxId) {
        return await this.excludeSandboxFromUsageOverview(cachedUsageOverview, excludeSandboxId)
      }

      return cachedUsageOverview
    }

    // cache miss, wait for lock
    const lockKey = `org:${organizationId}:fetch-sandbox-usage-from-db`
    await this.redisLockProvider.waitForLock(lockKey, 60)

    try {
      // check if cache was updated while waiting for lock
      cachedUsageOverview = await this.getCachedSandboxUsageOverview(organizationId)

      // cache hit
      if (cachedUsageOverview) {
        if (excludeSandboxId) {
          return await this.excludeSandboxFromUsageOverview(cachedUsageOverview, excludeSandboxId)
        }

        return cachedUsageOverview
      }

      // cache miss, fetch from db
      const usageOverview = await this.fetchSandboxUsageFromDb(organizationId)

      if (excludeSandboxId) {
        return await this.excludeSandboxFromUsageOverview(usageOverview, excludeSandboxId)
      }

      return usageOverview
    } finally {
      await this.redisLockProvider.unlock(lockKey)
    }
  }

  /**
   * Exclude the usage of a sandbox from the usage overview.
   *
   * @param usageOverview
   * @param excludeSandboxId
   */
  private async excludeSandboxFromUsageOverview(
    usageOverview: SandboxUsageOverviewInternalDto,
    excludeSandboxId: string,
  ): Promise<SandboxUsageOverviewInternalDto> {
    const excludedSandbox = await this.sandboxRepository.findOne({
      where: { id: excludeSandboxId },
    })

    if (!excludedSandbox) {
      return usageOverview
    }

    let cpuToSubtract = 0
    let memToSubtract = 0
    let diskToSubtract = 0

    if (SANDBOX_STATES_CONSUMING_COMPUTE.includes(excludedSandbox.state)) {
      cpuToSubtract = excludedSandbox.cpu
      memToSubtract = excludedSandbox.mem
    }

    if (SANDBOX_STATES_CONSUMING_DISK.includes(excludedSandbox.state)) {
      diskToSubtract = excludedSandbox.disk
    }

    return {
      ...usageOverview,
      currentCpuUsage: Math.max(0, usageOverview.currentCpuUsage - cpuToSubtract),
      currentMemoryUsage: Math.max(0, usageOverview.currentMemoryUsage - memToSubtract),
      currentDiskUsage: Math.max(0, usageOverview.currentDiskUsage - diskToSubtract),
    }
  }

  /**
   * Get the usage overview for all snapshot quotas of an organization.
   *
   * @param organizationId
   */
  async getSnapshotUsageOverview(organizationId: string): Promise<SnapshotUsageOverviewInternalDto> {
    let cachedUsageOverview = await this.getCachedSnapshotUsageOverview(organizationId)

    // cache hit
    if (cachedUsageOverview) {
      return cachedUsageOverview
    }

    // cache miss, wait for lock
    const lockKey = `org:${organizationId}:fetch-snapshot-usage-from-db`
    await this.redisLockProvider.waitForLock(lockKey, 60)

    try {
      // check if cache was updated while waiting for lock
      cachedUsageOverview = await this.getCachedSnapshotUsageOverview(organizationId)

      // cache hit
      if (cachedUsageOverview) {
        return cachedUsageOverview
      }

      // cache miss, fetch from db
      return await this.fetchSnapshotUsageFromDb(organizationId)
    } finally {
      await this.redisLockProvider.unlock(lockKey)
    }
  }

  /**
   * Get the usage overview for all volume quotas of an organization.
   *
   * @param organizationId
   */
  async getVolumeUsageOverview(organizationId: string): Promise<VolumeUsageOverviewInternalDto> {
    let cachedUsageOverview = await this.getCachedVolumeUsageOverview(organizationId)

    // cache hit
    if (cachedUsageOverview) {
      return cachedUsageOverview
    }

    // cache miss, wait for lock
    const lockKey = `org:${organizationId}:fetch-volume-usage-from-db`
    await this.redisLockProvider.waitForLock(lockKey, 60)

    try {
      // check if cache was updated while waiting for lock
      cachedUsageOverview = await this.getCachedVolumeUsageOverview(organizationId)

      // cache hit
      if (cachedUsageOverview) {
        return cachedUsageOverview
      }

      // cache miss, fetch from db
      return await this.fetchVolumeUsageFromDb(organizationId)
    } finally {
      await this.redisLockProvider.unlock(lockKey)
    }
  }

  // ================= TEST =========================

  /**
   * Get the current and pending usage overview for all sandbox quotas of an organization.
   *
   * @param organizationId
   * @param excludeSandboxId - If provided, the usage overview will be returned without the usage of the sandbox with the given ID
   */
  async getSandboxUsageOverviewWithPending(
    organizationId: string,
    excludeSandboxId?: string,
  ): Promise<SandboxUsageOverviewWithPendingInternalDto> {
    let cachedUsageOverview = await this.getCachedSandboxUsageOverviewWithPending(organizationId)

    // cache hit
    if (cachedUsageOverview) {
      if (excludeSandboxId) {
        return await this.excludeSandboxFromUsageOverviewWithPending(cachedUsageOverview, excludeSandboxId)
      }

      return cachedUsageOverview
    }

    // cache miss, wait for lock
    const lockKey = `org:${organizationId}:fetch-sandbox-usage-from-db`
    await this.redisLockProvider.waitForLock(lockKey, 60)

    try {
      // check if cache was updated while waiting for lock
      cachedUsageOverview = await this.getCachedSandboxUsageOverviewWithPending(organizationId)

      // cache hit
      if (cachedUsageOverview) {
        if (excludeSandboxId) {
          return await this.excludeSandboxFromUsageOverviewWithPending(cachedUsageOverview, excludeSandboxId)
        }

        return cachedUsageOverview
      }

      // cache miss, fetch from db
      const usageOverview = await this.fetchSandboxUsageFromDb(organizationId)

      // Get pending usage separately since it's not stored in DB
      const pendingUsageOverview = await this.getCachedSandboxPendingUsageOverview(organizationId)

      const combinedUsageOverview: SandboxUsageOverviewWithPendingInternalDto = {
        ...usageOverview,
        ...pendingUsageOverview,
      }

      if (excludeSandboxId) {
        return await this.excludeSandboxFromUsageOverviewWithPending(combinedUsageOverview, excludeSandboxId)
      }

      return combinedUsageOverview
    } finally {
      await this.redisLockProvider.unlock(lockKey)
    }
  }

  /**
   * Get the cached usage overview for all sandbox quotas of an organization, including pending usage.
   * Fetches both current and pending usage atomically with a single Lua script.
   *
   * @param organizationId
   */
  private async getCachedSandboxUsageOverviewWithPending(
    organizationId: string,
  ): Promise<SandboxUsageOverviewWithPendingInternalDto | null> {
    const script = `
    return {
      redis.call("GET", KEYS[1]),
      redis.call("GET", KEYS[2]),
      redis.call("GET", KEYS[3]),
      redis.call("GET", KEYS[4]),
      redis.call("GET", KEYS[5]),
      redis.call("GET", KEYS[6])
    }
  `

    const result = (await this.redis.eval(
      script,
      6,
      this.getQuotaUsageCacheKey(organizationId, 'cpu'),
      this.getQuotaUsageCacheKey(organizationId, 'memory'),
      this.getQuotaUsageCacheKey(organizationId, 'disk'),
      this.getPendingQuotaUsageCacheKey(organizationId, 'cpu'),
      this.getPendingQuotaUsageCacheKey(organizationId, 'memory'),
      this.getPendingQuotaUsageCacheKey(organizationId, 'disk'),
    )) as (string | null)[]

    const [cpuUsage, memoryUsage, diskUsage, pendingCpuUsage, pendingMemoryUsage, pendingDiskUsage] = result

    // If any current usage is null, consider it a cache miss
    if (cpuUsage === null || memoryUsage === null || diskUsage === null) {
      return null
    }

    // Check cache staleness for current usage
    const resourceType = getResourceTypeFromQuota('cpu') // 'sandbox' resource type
    const isStale = await this.isCacheStale(organizationId, resourceType)

    if (isStale) {
      return null
    }

    // Parse and validate current usage values
    const parsedCpuUsage = Number(cpuUsage)
    const parsedMemoryUsage = Number(memoryUsage)
    const parsedDiskUsage = Number(diskUsage)

    if (
      isNaN(parsedCpuUsage) ||
      parsedCpuUsage < 0 ||
      isNaN(parsedMemoryUsage) ||
      parsedMemoryUsage < 0 ||
      isNaN(parsedDiskUsage) ||
      parsedDiskUsage < 0
    ) {
      return null
    }

    // Parse pending usage values (null is acceptable)
    const parsedPendingCpuUsage = pendingCpuUsage ? Number(pendingCpuUsage) : null
    const parsedPendingMemoryUsage = pendingMemoryUsage ? Number(pendingMemoryUsage) : null
    const parsedPendingDiskUsage = pendingDiskUsage ? Number(pendingDiskUsage) : null

    // Validate pending usage values if they exist
    const validPendingCpuUsage =
      parsedPendingCpuUsage !== null && !isNaN(parsedPendingCpuUsage) && parsedPendingCpuUsage >= 0
        ? parsedPendingCpuUsage
        : null
    const validPendingMemoryUsage =
      parsedPendingMemoryUsage !== null && !isNaN(parsedPendingMemoryUsage) && parsedPendingMemoryUsage >= 0
        ? parsedPendingMemoryUsage
        : null
    const validPendingDiskUsage =
      parsedPendingDiskUsage !== null && !isNaN(parsedPendingDiskUsage) && parsedPendingDiskUsage >= 0
        ? parsedPendingDiskUsage
        : null

    return {
      currentCpuUsage: parsedCpuUsage,
      currentMemoryUsage: parsedMemoryUsage,
      currentDiskUsage: parsedDiskUsage,
      pendingCpuUsage: validPendingCpuUsage,
      pendingMemoryUsage: validPendingMemoryUsage,
      pendingDiskUsage: validPendingDiskUsage,
    }
  }

  /**
   * Get the cached pending usage overview for all sandbox quotas of an organization.
   *
   * @param organizationId
   */
  private async getCachedSandboxPendingUsageOverview(
    organizationId: string,
  ): Promise<SandboxPendingUsageOverviewInternalDto> {
    const pendingCpuUsage = await this.getQuotaPendingUsage(organizationId, 'cpu')
    const pendingMemoryUsage = await this.getQuotaPendingUsage(organizationId, 'memory')
    const pendingDiskUsage = await this.getQuotaPendingUsage(organizationId, 'disk')

    return {
      pendingCpuUsage,
      pendingMemoryUsage,
      pendingDiskUsage,
    }
  }

  /**
   * Exclude the usage of a sandbox from the usage overview with pending.
   *
   * @param usageOverview
   * @param excludeSandboxId
   */
  private async excludeSandboxFromUsageOverviewWithPending(
    usageOverview: SandboxUsageOverviewWithPendingInternalDto,
    excludeSandboxId: string,
  ): Promise<SandboxUsageOverviewWithPendingInternalDto> {
    const excludedSandbox = await this.sandboxRepository.findOne({
      where: { id: excludeSandboxId },
    })

    if (!excludedSandbox) {
      return usageOverview
    }

    let cpuToSubtract = 0
    let memToSubtract = 0
    let diskToSubtract = 0

    if (SANDBOX_STATES_CONSUMING_COMPUTE.includes(excludedSandbox.state)) {
      cpuToSubtract = excludedSandbox.cpu
      memToSubtract = excludedSandbox.mem
    }

    if (SANDBOX_STATES_CONSUMING_DISK.includes(excludedSandbox.state)) {
      diskToSubtract = excludedSandbox.disk
    }

    return {
      ...usageOverview,
      currentCpuUsage: Math.max(0, usageOverview.currentCpuUsage - cpuToSubtract),
      currentMemoryUsage: Math.max(0, usageOverview.currentMemoryUsage - memToSubtract),
      currentDiskUsage: Math.max(0, usageOverview.currentDiskUsage - diskToSubtract),
      // Pending usage is not affected by exclusions
    }
  }

  // ===== PRIVATE HELPERS FOR GETTING SANDBOX/SNAPSHOT/VOLUME USAGE OVERVIEWS FROM CACHE =====

  /**
   * Get the cached usage overview for all sandbox quotas of an organization.
   *
   * @param organizationId
   */
  private async getCachedSandboxUsageOverview(organizationId: string): Promise<SandboxUsageOverviewInternalDto | null> {
    const cpuUsage = await this.getQuotaUsageCachedValue(organizationId, 'cpu')
    const memoryUsage = await this.getQuotaUsageCachedValue(organizationId, 'memory')
    const diskUsage = await this.getQuotaUsageCachedValue(organizationId, 'disk')

    if (cpuUsage === null || memoryUsage === null || diskUsage === null) {
      return null
    }

    const script = `
      return {
        redis.call("GET", KEYS[1]),
        redis.call("GET", KEYS[2]),
        redis.call("GET", KEYS[3])
      }
    `

    const result = await this.redis.eval(
      script,
      3,
      this.getQuotaUsageCacheKey(organizationId, 'cpu'),
      this.getQuotaUsageCacheKey(organizationId, 'memory'),
      this.getQuotaUsageCacheKey(organizationId, 'disk'),
    )

    return {
      currentCpuUsage: result[0],
      currentMemoryUsage: result[1],
      currentDiskUsage: result[2],
    }
  }

  /**
   * Get the cached usage overview for all snapshot quotas of an organization.
   *
   * @param organizationId
   */
  private async getCachedSnapshotUsageOverview(
    organizationId: string,
  ): Promise<SnapshotUsageOverviewInternalDto | null> {
    const snapshotUsage = await this.getQuotaUsageCachedValue(organizationId, 'snapshot_count')

    if (snapshotUsage === null) {
      return null
    }

    return {
      currentSnapshotUsage: snapshotUsage,
    }
  }

  /**
   * Get the cached usage overview for all volume quotas of an organization.
   *
   * @param organizationId
   */
  private async getCachedVolumeUsageOverview(organizationId: string): Promise<VolumeUsageOverviewInternalDto | null> {
    const volumeUsage = await this.getQuotaUsageCachedValue(organizationId, 'volume_count')

    if (volumeUsage === null) {
      return null
    }

    return {
      currentVolumeUsage: volumeUsage,
    }
  }

  // ===== PRIVATE HELPERS FOR FETCHING SANDBOX/SNAPSHOT/VOLUME USAGE OVERVIEWS FROM DB AND CACHING THEM =====

  /**
   * Fetch the usage overview for all sandbox quotas of an organization from the database and cache the results.
   *
   * @param organizationId
   */
  async fetchSandboxUsageFromDb(organizationId: string): Promise<SandboxUsageOverviewInternalDto> {
    // fetch from db
    const sandboxUsageMetrics: {
      used_cpu: number
      used_mem: number
      used_disk: number
    } = await this.sandboxRepository
      .createQueryBuilder('sandbox')
      .select([
        'SUM(CASE WHEN sandbox.state IN (:...statesConsumingCompute) THEN sandbox.cpu ELSE 0 END) as used_cpu',
        'SUM(CASE WHEN sandbox.state IN (:...statesConsumingCompute) THEN sandbox.mem ELSE 0 END) as used_mem',
        'SUM(CASE WHEN sandbox.state IN (:...statesConsumingDisk) THEN sandbox.disk ELSE 0 END) as used_disk',
      ])
      .where('sandbox.organizationId = :organizationId', { organizationId })
      .setParameter('statesConsumingCompute', SANDBOX_STATES_CONSUMING_COMPUTE)
      .setParameter('statesConsumingDisk', SANDBOX_STATES_CONSUMING_DISK)
      .getRawOne()

    const cpuUsage = Number(sandboxUsageMetrics.used_cpu) || 0
    const memoryUsage = Number(sandboxUsageMetrics.used_mem) || 0
    const diskUsage = Number(sandboxUsageMetrics.used_disk) || 0

    // cache the results
    const cpuCacheKey = this.getQuotaUsageCacheKey(organizationId, 'cpu')
    const memoryCacheKey = this.getQuotaUsageCacheKey(organizationId, 'memory')
    const diskCacheKey = this.getQuotaUsageCacheKey(organizationId, 'disk')

    await this.redis
      .multi()
      .setex(cpuCacheKey, this.CACHE_TTL_SECONDS, cpuUsage)
      .setex(memoryCacheKey, this.CACHE_TTL_SECONDS, memoryUsage)
      .setex(diskCacheKey, this.CACHE_TTL_SECONDS, diskUsage)
      .exec()

    await this.resetCacheStaleness(organizationId, 'sandbox')

    return {
      currentCpuUsage: cpuUsage,
      currentMemoryUsage: memoryUsage,
      currentDiskUsage: diskUsage,
    }
  }

  /**
   * Fetch the usage overview for all snapshot quotas of an organization from the database and cache the results.
   *
   * @param organizationId
   */
  private async fetchSnapshotUsageFromDb(organizationId: string): Promise<SnapshotUsageOverviewInternalDto> {
    // fetch from db
    const snapshotUsage = await this.snapshotRepository.count({
      where: {
        organizationId,
        state: Not(In(SNAPSHOT_USAGE_IGNORED_STATES)),
      },
    })

    // cache the result
    const cacheKey = this.getQuotaUsageCacheKey(organizationId, 'snapshot_count')
    await this.redis.setex(cacheKey, this.CACHE_TTL_SECONDS, snapshotUsage)

    await this.resetCacheStaleness(organizationId, 'snapshot')

    return {
      currentSnapshotUsage: snapshotUsage,
    }
  }

  /**
   * Fetch the usage overview for all volume quotas of an organization from the database and cache the results.
   *
   * @param organizationId
   */
  private async fetchVolumeUsageFromDb(organizationId: string): Promise<VolumeUsageOverviewInternalDto> {
    // fetch from db
    const volumeUsage = await this.volumeRepository.count({
      where: {
        organizationId,
        state: Not(In(VOLUME_USAGE_IGNORED_STATES)),
      },
    })

    // cache the result
    const cacheKey = this.getQuotaUsageCacheKey(organizationId, 'volume_count')
    await this.redis.setex(cacheKey, this.CACHE_TTL_SECONDS, volumeUsage)

    await this.resetCacheStaleness(organizationId, 'volume')

    return {
      currentVolumeUsage: volumeUsage,
    }
  }

  // ===== PRIVATE HELPERS FOR CACHED QUOTA USAGE VALUES =====

  /**
   * Get the cache key for a quota usage value.
   *
   * @param organizationId
   * @param quotaType
   */
  private getQuotaUsageCacheKey(organizationId: string, quotaType: OrganizationUsageQuotaType): string {
    return `org:${organizationId}:quota:${quotaType}:usage`
  }

  /**
   * Get the cached usage value for a quota of an organization.
   *
   * @param organizationId
   * @param quotaType
   * @returns The cached value for the quota usage, or `null` if the cache is not present or the value is not a non-negative number
   */
  private async getQuotaUsageCachedValue(
    organizationId: string,
    quotaType: OrganizationUsageQuotaType,
  ): Promise<number | null> {
    const cacheKey = this.getQuotaUsageCacheKey(organizationId, quotaType)
    const cachedData = await this.redis.get(cacheKey)

    if (!cachedData) {
      return null
    }

    // must be a non-negative number
    const parsedValue = Number(cachedData)
    if (isNaN(parsedValue) || parsedValue < 0) {
      return null
    }

    const resourceType = getResourceTypeFromQuota(quotaType)
    const isStale = await this.isCacheStale(organizationId, resourceType)

    if (isStale) {
      return null
    }

    return parsedValue
  }

  /**
   * Updates the quota usage in the cache. If cache is not present, this method is a no-op.
   *
   * If the corresponding quota type has pending usage in the cache and the delta is positive, the pending usage is decremented accordingly.
   *
   * @param organizationId
   * @param quotaType
   * @param delta
   */
  private async updateQuotaUsage(
    organizationId: string,
    quotaType: OrganizationUsageQuotaType,
    delta: number,
  ): Promise<void> {
    const script = `
      local cacheKey = KEYS[1]
      local pendingCacheKey = KEYS[2]
      local delta = tonumber(ARGV[1])
      local ttl = tonumber(ARGV[2])

      if redis.call("EXISTS", cacheKey) == 1 then
        redis.call("INCRBY", cacheKey, delta)
        redis.call("EXPIRE", cacheKey, ttl)
      end
      
      local pending = tonumber(redis.call("GET", pendingCacheKey))
      if pending and tonumber(pending) > 0 and delta > 0 then
        redis.call("DECRBY", pendingCacheKey, delta)
      end

      return {
        redis.call("GET", cacheKey),
        redis.call("GET", pendingCacheKey)
      }
    `

    const cacheKey = this.getQuotaUsageCacheKey(organizationId, quotaType)
    const pendingCacheKey = this.getPendingQuotaUsageCacheKey(organizationId, quotaType)

    const result = await this.redis.eval(
      script,
      2,
      cacheKey,
      pendingCacheKey,
      delta.toString(),
      this.CACHE_TTL_SECONDS.toString(),
    )

    this.logger.log(`=== updateQuotaUsage -> result: ${result}`)
  }

  // ===== HELPERS FOR PENDING QUOTA USAGE =====

  /**
   * Get the cache key for pending usage of an organization's quota.
   *
   * @param organizationId
   * @param quotaType
   */
  private getPendingQuotaUsageCacheKey(organizationId: string, quotaType: OrganizationUsageQuotaType): string {
    return `org:${organizationId}:pending-${quotaType}`
  }

  /**
   * Get the pending usage for a quota of an organization.
   *
   * @param organizationId
   * @param quotaType
   * @returns The pending usage for the quota, or `null` if the cache is not present or the value is not a non-negative number
   */
  async getQuotaPendingUsage(organizationId: string, quotaType: OrganizationUsageQuotaType): Promise<number | null> {
    const cacheKey = this.getPendingQuotaUsageCacheKey(organizationId, quotaType)
    const cachedData = await this.redis.get(cacheKey)

    if (!cachedData) {
      return null
    }

    // must be a non-negative number
    const parsedValue = Number(cachedData)
    if (isNaN(parsedValue) || parsedValue < 0) {
      return null
    }

    return parsedValue
  }

  /**
   * Increments the pending usage for sandbox quotas in an organization.
   *
   * Pending usage is used to protect against race conditions to prevent quota abuse.
   *
   * If a user action will result in increased quota usage, we will first increment the pending usage.
   *
   * When the user action is complete, this pending usage will be transfered to the actual usage.
   *
   * As a safeguard, an expiration time is set on the pending usage cache to prevent lockout for new operations.
   *
   * @param organizationId
   * @param cpu - The amount of CPU to increment.
   * @param memory - The amount of memory to increment.
   * @param disk - The amount of disk to increment.
   * @param excludeSandboxId - If provided, pending usage will be incremented only for quotas that are not consumed by the sandbox in its current state.
   * @returns an object with the boolean values indicating if the pending usage was incremented for each quota type
   */
  async incrementPendingSandboxUsage(
    organizationId: string,
    cpu: number,
    memory: number,
    disk: number,
    excludeSandboxId?: string,
  ): Promise<{
    cpuIncremented: boolean
    memoryIncremented: boolean
    diskIncremented: boolean
  }> {
    // determine for which quota types we should increment the pending usage
    let shouldIncrementCpu = true
    let shouldIncrementMemory = true
    let shouldIncrementDisk = true

    if (excludeSandboxId) {
      const excludedSandbox = await this.sandboxRepository.findOne({
        where: { id: excludeSandboxId },
      })

      if (excludedSandbox) {
        if (SANDBOX_STATES_CONSUMING_COMPUTE.includes(excludedSandbox.state)) {
          shouldIncrementCpu = false
          shouldIncrementMemory = false
        }

        if (SANDBOX_STATES_CONSUMING_DISK.includes(excludedSandbox.state)) {
          shouldIncrementDisk = false
        }
      }
    }

    // increment the pending usage for necessary quota types
    const script = `
      local cpuKey = KEYS[1]
      local memoryKey = KEYS[2]
      local diskKey = KEYS[3]

      local shouldIncrementCpu = ARGV[1] == "true"
      local shouldIncrementMemory = ARGV[2] == "true"
      local shouldIncrementDisk = ARGV[3] == "true"

      local cpuIncrement = tonumber(ARGV[4])
      local memoryIncrement = tonumber(ARGV[5])
      local diskIncrement = tonumber(ARGV[6])

      local ttl = tonumber(ARGV[7])
    
      if shouldIncrementCpu then
        redis.call("INCRBY", cpuKey, cpuIncrement)
        redis.call("EXPIRE", cpuKey, ttl)
      end

      if shouldIncrementMemory then
        redis.call("INCRBY", memoryKey, memoryIncrement)
        redis.call("EXPIRE", memoryKey, ttl)
      end

      if shouldIncrementDisk then
        redis.call("INCRBY", diskKey, diskIncrement)
        redis.call("EXPIRE", diskKey, ttl)
      end

      return {
        redis.call("GET", cpuKey),
        redis.call("GET", memoryKey),
        redis.call("GET", diskKey)
      }
    `

    const result = await this.redis.eval(
      script,
      3,
      this.getPendingQuotaUsageCacheKey(organizationId, 'cpu'),
      this.getPendingQuotaUsageCacheKey(organizationId, 'memory'),
      this.getPendingQuotaUsageCacheKey(organizationId, 'disk'),
      shouldIncrementCpu.toString(),
      shouldIncrementMemory.toString(),
      shouldIncrementDisk.toString(),
      cpu.toString(),
      memory.toString(),
      disk.toString(),
      this.CACHE_TTL_SECONDS.toString(),
    )

    //this.logger.warn(`+++ incrementPendingSandboxUsage -> result: ${result}`)

    return {
      cpuIncremented: shouldIncrementCpu,
      memoryIncremented: shouldIncrementMemory,
      diskIncremented: shouldIncrementDisk,
    }
  }

  /**
   * Decrements the pending usage for sandbox quotas in an organization.
   *
   * Use this method to roll back pending usage after incrementing it for an action that was subsequently rejected.
   *
   * Pending usage is used to protect against race conditions to prevent quota abuse.
   *
   * If a user action will result in increased quota usage, we will first increment the pending usage.
   *
   * When the user action is complete, this pending usage will be transfered to the actual usage.
   *
   * @param organizationId
   * @param cpu - If provided, the amount of CPU to decrement.
   * @param memory - If provided, the amount of memory to decrement.
   * @param disk - If provided, the amount of disk to decrement.
   */
  async decrementPendingSandboxUsage(
    organizationId: string,
    cpu?: number,
    memory?: number,
    disk?: number,
  ): Promise<void> {
    // decrement the pending usage for necessary quota types
    const script = `
      local cpuKey = KEYS[1]
      local memoryKey = KEYS[2] 
      local diskKey = KEYS[3]

      local cpuDecrement = tonumber(ARGV[1])
      local memoryDecrement = tonumber(ARGV[2])
      local diskDecrement = tonumber(ARGV[3])
      
      if cpuDecrement then
        redis.call("DECRBY", cpuKey, cpuDecrement)
      end

      if memoryDecrement then
        redis.call("DECRBY", memoryKey, memoryDecrement)
      end

      if diskDecrement then
        redis.call("DECRBY", diskKey, diskDecrement)
      end

      return {
        redis.call("GET", cpuKey),
        redis.call("GET", memoryKey),
        redis.call("GET", diskKey)
      }
    `

    const result = await this.redis.eval(
      script,
      3,
      this.getPendingQuotaUsageCacheKey(organizationId, 'cpu'),
      this.getPendingQuotaUsageCacheKey(organizationId, 'memory'),
      this.getPendingQuotaUsageCacheKey(organizationId, 'disk'),
      cpu?.toString() ?? '0',
      memory?.toString() ?? '0',
      disk?.toString() ?? '0',
    )

    //this.logger.warn(`--- decrementPendingSandboxUsage -> result: ${result}`)
  }

  // ===== PRIVATE HELPERS FOR QUOTA USAGE CACHE STALENESS =====

  /**
   * Get the cache key for the staleness of the cached usage of an organization's resource quotas.
   *
   * @param organizationId
   * @param resourceType
   */
  private getCacheStalenessKey(organizationId: string, resourceType: OrganizationUsageResourceType): string {
    return `org:${organizationId}:resource:${resourceType}:usage:fetched_at`
  }

  /**
   * Reset the staleness of the cached usage of an organization's resource quotas.
   *
   * @param organizationId
   * @param resourceType
   */
  private async resetCacheStaleness(
    organizationId: string,
    resourceType: OrganizationUsageResourceType,
  ): Promise<void> {
    const cacheKey = this.getCacheStalenessKey(organizationId, resourceType)
    await this.redis.set(cacheKey, Date.now())
  }

  /**
   * Check if the cached usage of an organization's resource quotas is stale.
   *
   * @param organizationId
   * @param resourceType
   * @returns `true` if the cached usage is stale, `false` otherwise
   */
  private async isCacheStale(organizationId: string, resourceType: OrganizationUsageResourceType): Promise<boolean> {
    const cacheKey = this.getCacheStalenessKey(organizationId, resourceType)
    const cachedData = await this.redis.get(cacheKey)

    if (!cachedData) {
      return true
    }

    const lastFetchedAtTimestamp = Number(cachedData)
    if (isNaN(lastFetchedAtTimestamp)) {
      return true
    }

    return Date.now() - lastFetchedAtTimestamp > this.CACHE_MAX_AGE_MS
  }

  // ===== EVENT HANDLERS FOR UPDATING QUOTA USAGE IN CACHE =====

  @OnEvent(SandboxEvents.CREATED)
  async handleSandboxCreated(event: SandboxCreatedEvent) {
    const lockKey = `sandbox:${event.sandbox.id}:quota-usage-update`
    await this.redisLockProvider.waitForLock(lockKey, 60)

    try {
      await this.updateQuotaUsage(event.sandbox.organizationId, 'cpu', event.sandbox.cpu)
      await this.updateQuotaUsage(event.sandbox.organizationId, 'memory', event.sandbox.mem)
      await this.updateQuotaUsage(event.sandbox.organizationId, 'disk', event.sandbox.disk)
    } catch (error) {
      this.logger.warn(
        `Error updating cached sandbox quota usage for organization ${event.sandbox.organizationId}`,
        error,
      )
    } finally {
      await this.redisLockProvider.unlock(lockKey)
    }
  }

  @OnEvent(SandboxEvents.STATE_UPDATED)
  async handleSandboxStateUpdated(event: SandboxStateUpdatedEvent) {
    const lockKey = `sandbox:${event.sandbox.id}:quota-usage-update`
    await this.redisLockProvider.waitForLock(lockKey, 60)

    try {
      const cpuDelta = this.calculateQuotaUsageDelta(
        event.sandbox.cpu,
        event.oldState,
        event.newState,
        SANDBOX_STATES_CONSUMING_COMPUTE,
      )

      const memDelta = this.calculateQuotaUsageDelta(
        event.sandbox.mem,
        event.oldState,
        event.newState,
        SANDBOX_STATES_CONSUMING_COMPUTE,
      )

      const diskDelta = this.calculateQuotaUsageDelta(
        event.sandbox.disk,
        event.oldState,
        event.newState,
        SANDBOX_STATES_CONSUMING_DISK,
      )

      if (cpuDelta !== 0) {
        await this.updateQuotaUsage(event.sandbox.organizationId, 'cpu', cpuDelta)
      }

      if (memDelta !== 0) {
        await this.updateQuotaUsage(event.sandbox.organizationId, 'memory', memDelta)
      }

      if (diskDelta !== 0) {
        await this.updateQuotaUsage(event.sandbox.organizationId, 'disk', diskDelta)
      }
    } catch (error) {
      this.logger.warn(
        `Error updating cached sandbox quota usage for organization ${event.sandbox.organizationId}`,
        error,
      )
    } finally {
      await this.redisLockProvider.unlock(lockKey)
    }
  }

  @OnEvent(SnapshotEvents.CREATED)
  async handleSnapshotCreated(event: SnapshotCreatedEvent) {
    const lockKey = `snapshot:${event.snapshot.id}:quota-usage-update`
    await this.redisLockProvider.waitForLock(lockKey, 60)

    try {
      await this.updateQuotaUsage(event.snapshot.organizationId, 'snapshot_count', 1)
    } catch (error) {
      this.logger.warn(
        `Error updating cached snapshot quota usage for organization ${event.snapshot.organizationId}`,
        error,
      )
    } finally {
      await this.redisLockProvider.unlock(lockKey)
    }
  }

  @OnEvent(SnapshotEvents.STATE_UPDATED)
  async handleSnapshotStateUpdated(event: SnapshotStateUpdatedEvent) {
    const lockKey = `snapshot:${event.snapshot.id}:quota-usage-update`
    await this.redisLockProvider.waitForLock(lockKey, 60)

    try {
      const countDelta = this.calculateQuotaUsageDelta(1, event.oldState, event.newState, SNAPSHOT_USAGE_IGNORED_STATES)

      if (countDelta !== 0) {
        await this.updateQuotaUsage(event.snapshot.organizationId, 'snapshot_count', countDelta)
      }
    } catch (error) {
      this.logger.warn(
        `Error updating cached snapshot quota usage for organization ${event.snapshot.organizationId}`,
        error,
      )
    } finally {
      await this.redisLockProvider.unlock(lockKey)
    }
  }

  @OnEvent(VolumeEvents.CREATED)
  async handleVolumeCreated(event: VolumeCreatedEvent) {
    const lockKey = `volume:${event.volume.id}:quota-usage-update`
    await this.redisLockProvider.waitForLock(lockKey, 60)

    try {
      await this.updateQuotaUsage(event.volume.organizationId, 'volume_count', 1)
    } catch (error) {
      this.logger.warn(
        `Error updating cached volume quota usage for organization ${event.volume.organizationId}`,
        error,
      )
    } finally {
      await this.redisLockProvider.unlock(lockKey)
    }
  }

  @OnEvent(VolumeEvents.STATE_UPDATED)
  async handleVolumeStateUpdated(event: VolumeStateUpdatedEvent) {
    const lockKey = `volume:${event.volume.id}:quota-usage-update`
    await this.redisLockProvider.waitForLock(lockKey, 60)

    try {
      const countDelta = this.calculateQuotaUsageDelta(1, event.oldState, event.newState, VOLUME_USAGE_IGNORED_STATES)

      if (countDelta !== 0) {
        await this.updateQuotaUsage(event.volume.organizationId, 'volume_count', countDelta)
      }
    } catch (error) {
      this.logger.warn(
        `Error updating cached volume quota usage for organization ${event.volume.organizationId}`,
        error,
      )
    } finally {
      await this.redisLockProvider.unlock(lockKey)
    }
  }

  private calculateQuotaUsageDelta<TState>(
    resourceAmount: number,
    oldState: TState,
    newState: TState,
    statesConsumingResource: TState[],
  ): number {
    const wasConsumingResource = statesConsumingResource.includes(oldState)
    const isConsumingResource = statesConsumingResource.includes(newState)

    if (!wasConsumingResource && isConsumingResource) {
      return resourceAmount
    }

    if (wasConsumingResource && !isConsumingResource) {
      return -resourceAmount
    }

    return 0
  }
}
