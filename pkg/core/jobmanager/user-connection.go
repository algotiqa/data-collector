//=============================================================================
//===
//=== Copyright (C) 2025-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package jobmanager

//=============================================================================

type UserConnection struct {
	username       string
	connectionCode string
	connected      bool
	scheduledJob   *ScheduledJob
}

//=============================================================================

func (uc *UserConnection) key() string {
	return uc.username + ":" + uc.connectionCode
}

//=============================================================================

func (uc *UserConnection) allocateToJob(job *ScheduledJob) {
	uc.scheduledJob        = job
	job.job.UserConnection = uc.key()
	job.job.Error          = ""
}

//=============================================================================

func (uc *UserConnection) isAllocated() bool {
	return uc.scheduledJob != nil
}

//=============================================================================

func (uc *UserConnection) deallocate() {
	job := uc.scheduledJob
	job.job.UserConnection = ""
	uc.scheduledJob        = nil
}

//=============================================================================

func newUserConnection(username, connectionCode string, connected bool) *UserConnection {
	return &UserConnection{
		username      : username,
		connectionCode: connectionCode,
		connected     : connected,
	}
}

//=============================================================================
