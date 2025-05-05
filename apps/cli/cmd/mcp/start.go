// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package mcp

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/daytonaio/daytona-ai-saas/cli/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/spf13/cobra"
)

var StartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start Daytona MCP Server",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		s := mcp.NewDaytonaMCPServer()

		interruptChan := make(chan os.Signal, 1)
		signal.Notify(interruptChan, os.Interrupt)

		errChan := make(chan error)

		if transport == "stdio" {
			go func() {
				errChan <- server.ServeStdio(&s.MCPServer)
			}()

			select {
			case err := <-errChan:
				return err
			case <-interruptChan:
				return nil
			}
		} else if transport == "sse" {
			sseServer := server.NewSSEServer(&s.MCPServer)

			go func() {
				errChan <- sseServer.Start("localhost:3004")
			}()

			select {
			case err := <-errChan:
				return err
			case <-interruptChan:
				return nil
			}
		} else {
			return fmt.Errorf("invalid transport: %s - valid transports are 'stdio' and 'sse'", transport)
		}
	},
}

var transport string

func init() {
	StartCmd.Flags().StringVarP(&transport, "transport", "t", "stdio", "Transport to use for the server")
}
