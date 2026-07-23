//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================

package main

import (
	"log/slog"

	"github.com/algotiqa/core/auth"
	"github.com/algotiqa/core/boot"
	"github.com/algotiqa/core/dbms"
	"github.com/algotiqa/core/msg"
	"github.com/algotiqa/core/req"
	"github.com/algotiqa/data-collector/pkg/app"
	"github.com/algotiqa/data-collector/pkg/core/jobmanager"
	"github.com/algotiqa/data-collector/pkg/core/messaging"
	"github.com/algotiqa/data-collector/pkg/core/process"
	"github.com/algotiqa/data-collector/pkg/ds"
	"github.com/algotiqa/data-collector/pkg/platform"
	"github.com/algotiqa/data-collector/pkg/service"
)

//=============================================================================

const component = "data-collector"
var   version   = "dev"

//=============================================================================

func main() {
	cfg := &app.Config{}
	boot.ReadConfig(component, cfg)
	logger := boot.InitLogger(component, version, &cfg.Application)
	engine := boot.InitEngine(logger, &cfg.Application)
	initClients()
	dbms.InitDatabase(&cfg.Database)
	ds.InitDatastore(&cfg.Datastore)
	platform.Init(&cfg.Platform)
	auth.InitAuthentication(&cfg.Authentication)
	msg.InitMessaging(&cfg.Messaging)
	service.Init(engine, cfg, logger)
	messaging.InitMessageListener()
	process.Init(cfg)
	jobmanager.Init(cfg)
	boot.RunHttpServer(engine, &cfg.Application)
}

//=============================================================================

func initClients() {
	slog.Info("Initializing clients...")
	req.AddDefaultClient("ca.crt", "server.crt", "server.key")
}

//=============================================================================
