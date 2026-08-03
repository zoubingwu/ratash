use std::collections::VecDeque;
use std::io;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use ratash::application::{
    ApplicationClient, ApplicationError, ApplicationOperation, ApplicationOutput, LatencyFreshness,
    LatencyProbeStatus, LifecycleAction, LifecycleOutcome, PolicyTargetValidation,
    ProfileListOutcome, ProxyAvailability, ProxyGroupSummary, ProxyListOutcome, ProxyMemberKind,
    ProxyNodeRow, RecoveryOutcome, RecoveryStatus, RuleListOutcome, RuleMutationAction,
    RuleMutationOutcome, RulePlacement, RuleSummary, RuntimeApplyOutcome, RuntimeApplyStatus,
};
use ratash::constants::LOG_CAPACITY;
use ratash::domain::{
    ActiveProfileSummary, ApplyState, CoreLifecycle, CoreStatus, LocalRuleSetRevision,
    NodeRecordId, ProbeQueueStatus, ProfileId, ProxyGroupId, RuntimeApplySnapshot, SampleState,
    StatusSnapshot, StreamHealthSet, StreamState, SupervisorLifecycle, SupervisorStatus,
    TrafficSample, TunStatus,
};
use ratash::ipc::RequestId;
use ratash::tui::{
    Command, FullViewSnapshot, KeyInput, MutationSuccess, ProfileRow, ProxyGroupRow,
    ProxyGroupSnapshot, ProxyRow, RuleListSnapshot, RuleRow, TerminalAction, TerminalControl,
    TerminalInput, UiEvent, UiIntent, ViewLogRecord,
};
use ratash::tui_runtime::{
    ApplicationCommandExecutor, ApplicationSnapshotSource, BackgroundCommandDispatcher,
    BoundedReconnectTimer, CancellationToken, CommandDispatchError, CommandDispatcher,
    DispatchedEvent, FullSnapshotSource, LogTail, NoShutdownSignal, RatatuiStatusRenderer,
    ReconnectTiming, RenderedFrame, RuntimeClock, RuntimeWaiter, RuntimeWaker, ShutdownSignal,
    StatusInterfaceError, StatusInterfaceErrorKind, StatusInterfacePorts, StatusInterfaceRuntime,
    StatusInterfaceSources, StatusLogEvent, StatusLogEventSource, StatusRenderer,
    TerminalEventSource, UiCommandExecutor, bootstrap_status_interface, run_with_terminal_session,
};
use ratatui::backend::TestBackend;

#[test]
fn reconnect_backoff_grows_and_stays_within_the_product_bound() {
    let mut timer = BoundedReconnectTimer::new(Duration::from_millis(250), Duration::from_secs(10))
        .expect("product reconnect bounds should be valid");

    timer.schedule(4, Duration::from_secs(1));
    assert_eq!(timer.deadline(), Some(Duration::from_millis(1_250)));
    assert_eq!(timer.take_due(Duration::from_millis(1_249)), None);
    assert_eq!(timer.take_due(Duration::from_millis(1_250)), Some(4));

    for generation in 5..=12 {
        timer.schedule(generation, Duration::from_secs(2));
        assert!(
            timer
                .deadline()
                .expect("scheduled reconnect has a deadline")
                <= Duration::from_secs(12)
        );
        let _ = timer.take_due(Duration::from_secs(12));
    }

    timer.reset();
    timer.schedule(13, Duration::from_secs(20));
    assert_eq!(timer.deadline(), Some(Duration::from_millis(20_250)));
}

#[test]
fn bootstrap_connects_and_fetches_before_terminal_takeover() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(FakeEvents::with_order(Arc::clone(&order)));
    let snapshots = Arc::new(FakeSnapshots {
        snapshot: snapshot(10),
        order: Arc::clone(&order),
        fail: false,
    });
    let sources = StatusInterfaceSources {
        snapshots,
        events: events.clone(),
        commands: Arc::new(ImmediateCommands),
    };

    let initial = bootstrap_status_interface(&sources, 3)
        .expect("fixture bootstrap should complete before terminal setup");

    assert_eq!(initial.status.traffic.upload_bytes_per_second, 10);
    assert_eq!(
        order
            .lock()
            .expect("order lock should be available")
            .as_slice(),
        ["connect", "snapshot"]
    );
    assert!(events.disconnects().is_empty());
}

#[test]
fn failed_bootstrap_disconnects_without_entering_the_terminal() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(FakeEvents::with_order(Arc::clone(&order)));
    let sources = StatusInterfaceSources {
        snapshots: Arc::new(FakeSnapshots {
            snapshot: snapshot(0),
            order,
            fail: true,
        }),
        events: events.clone(),
        commands: Arc::new(ImmediateCommands),
    };

    let error = bootstrap_status_interface(&sources, 7)
        .expect_err("injected snapshot failure should stop bootstrap");

    assert_eq!(error.kind, StatusInterfaceErrorKind::Snapshot);
    assert_eq!(events.disconnects(), vec![7]);
}

#[test]
fn application_snapshot_adapter_reads_the_complete_initial_view() {
    let client = Arc::new(SnapshotClient::default());
    let events = Arc::new(FakeEvents::default());
    let source = ApplicationSnapshotSource::new(client.clone(), events);

    let full = source
        .fetch_full_snapshot(8, &CancellationToken::default())
        .expect("application outputs should form a complete snapshot");

    assert_eq!(full.status.traffic.upload_bytes_per_second, 55);
    assert!(full.profiles.is_empty());
    assert!(full.proxies.is_empty());
    assert_eq!(
        client
            .operations
            .lock()
            .expect("operation lock should be available")
            .as_slice(),
        [
            ApplicationOperation::GetStatus,
            ApplicationOperation::ProfileList
        ]
    );
}

#[test]
fn background_snapshot_adapter_skips_the_core_log_tail() {
    let client = Arc::new(SnapshotClient::default());
    let events = Arc::new(FakeEvents::default());
    let source = ApplicationSnapshotSource::new(client, events.clone());

    let refreshed = source
        .refresh_view_snapshot(8, &CancellationToken::default())
        .expect("background collection snapshot should load");

    assert!(refreshed.logs.is_empty());
    assert!(events.tail_requests().is_empty());
}

#[test]
fn application_snapshot_adapter_loads_proxy_groups_on_demand() {
    let client = Arc::new(ProxySnapshotClient::default());
    let events = Arc::new(FakeEvents::default());
    let source = ApplicationSnapshotSource::new(client.clone(), events);

    let full = source
        .fetch_full_snapshot(8, &CancellationToken::default())
        .expect("initial Proxy Group should load");
    assert_eq!(
        full.proxy_groups
            .iter()
            .map(|group| group.name.as_str())
            .collect::<Vec<_>>(),
        vec!["Automatic", "Manual"]
    );
    assert_eq!(full.proxies[0].name, "Tokyo");

    let manual = source
        .fetch_proxy_group("Manual", 8, &CancellationToken::default())
        .expect("selected Proxy Group should load on demand");
    assert_eq!(manual.group.name, "Manual");
    assert_eq!(manual.proxies[0].name, "Paris");
    assert_eq!(manual.proxies[0].freshness, LatencyFreshness::Fresh);
    assert_eq!(
        manual.proxies[0].probe_status,
        LatencyProbeStatus::Succeeded
    );
    assert_eq!(
        client
            .operations
            .lock()
            .expect("operation lock should be available")
            .iter()
            .filter_map(|operation| match operation {
                ApplicationOperation::ProxyList { group } => Some(group.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec!["Automatic", "Manual"]
    );
}

#[test]
fn application_snapshot_adapter_loads_rules_on_demand() {
    let client = Arc::new(SnapshotClient::default());
    let events = Arc::new(FakeEvents::default());
    let source = ApplicationSnapshotSource::new(client.clone(), events);

    let rules = source
        .fetch_rules(8, &CancellationToken::default())
        .expect("Local Rule Set should load on demand");

    assert!(rules.initialized);
    assert_eq!(rules.revision, Some(LocalRuleSetRevision(9)));
    assert_eq!(rules.rows.len(), 1);
    assert_eq!(rules.rows[0].rule_type, "DOMAIN-SUFFIX");
    assert_eq!(rules.rows[0].payload.as_deref(), Some("example.com"));
    assert_eq!(rules.rows[0].policy_target, "PROXY");
    assert_eq!(
        rules.rows[0].policy_target_validation,
        PolicyTargetValidation::Valid
    );
    assert_eq!(
        client
            .operations
            .lock()
            .expect("operation lock should be available")
            .last(),
        Some(&ApplicationOperation::RuleList)
    );
}

#[test]
fn event_loop_coalesces_status_updates_into_one_frame() {
    let events = Arc::new(FakeEvents::default());
    for upload in [20, 30, 40] {
        events.push(StatusLogEvent::Status {
            connection_generation: 1,
            status: Box::new(status(upload)),
        });
    }
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect = PassiveReconnect;
    let mut input = ScriptedInput::quit_on_poll(2);
    let clock = FixedClock(Duration::from_secs(1));
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();
    let waker = RuntimeWaker::default();

    {
        let mut runtime = StatusInterfaceRuntime::new(
            1,
            snapshot(10),
            StatusInterfacePorts {
                events,
                dispatcher: &mut dispatcher,
                reconnect: &mut reconnect,
                input: &mut input,
                waiter: &waker,
                waker: waker.clone(),
                clock: &clock,
                signal: &signal,
                renderer: &mut renderer,
            },
        );
        runtime.run().expect("scripted TUI run should exit cleanly");
        assert_eq!(
            runtime
                .state()
                .status
                .as_ref()
                .expect("runtime should retain the latest status")
                .traffic
                .upload_bytes_per_second,
            40
        );
    }

    assert_eq!(renderer.uploads, vec![10, 40]);
    assert!(dispatcher.cancelled_all);
}

#[test]
fn idle_event_loop_waits_without_a_resident_deadline_or_extra_frame() {
    let events = Arc::new(FakeEvents::default());
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect = PassiveReconnect;
    let mut input = ScriptedInput::quit_on_poll(2);
    let clock = FixedClock(Duration::from_secs(1));
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();
    let waiter = RecordingWaiter::default();
    let waker = RuntimeWaker::default();

    let mut runtime = StatusInterfaceRuntime::new(
        1,
        snapshot(10),
        StatusInterfacePorts {
            events,
            dispatcher: &mut dispatcher,
            reconnect: &mut reconnect,
            input: &mut input,
            waiter: &waiter,
            waker,
            clock: &clock,
            signal: &signal,
            renderer: &mut renderer,
        },
    );
    runtime
        .run()
        .expect("scripted idle wait should exit cleanly");
    drop(runtime);

    assert_eq!(waiter.waits(), vec![None]);
    assert_eq!(renderer.uploads, vec![10]);
}

#[test]
fn runtime_waker_preserves_a_wakeup_that_arrives_before_wait() {
    let waker = RuntimeWaker::default();
    let checkpoint = waker.checkpoint();

    waker.wake();
    waker.wait(checkpoint, None);

    assert_ne!(waker.checkpoint(), checkpoint);
}

#[test]
fn disconnected_event_loop_waits_until_the_exact_reconnect_deadline() {
    let events = Arc::new(FakeEvents::default());
    events.push(StatusLogEvent::Disconnected {
        connection_generation: 1,
    });
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect =
        BoundedReconnectTimer::new(Duration::from_millis(250), Duration::from_secs(10))
            .expect("fixture reconnect bounds should be valid");
    let mut input = ScriptedInput::quit_on_poll(3);
    let clock = FixedClock(Duration::from_secs(1));
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();
    let waiter = RecordingWaiter::default();
    let waker = RuntimeWaker::default();

    let mut runtime = StatusInterfaceRuntime::new(
        1,
        snapshot(10),
        StatusInterfacePorts {
            events,
            dispatcher: &mut dispatcher,
            reconnect: &mut reconnect,
            input: &mut input,
            waiter: &waiter,
            waker,
            clock: &clock,
            signal: &signal,
            renderer: &mut renderer,
        },
    );
    runtime
        .run()
        .expect("scripted reconnect wait should exit cleanly");
    drop(runtime);

    assert_eq!(waiter.waits(), vec![Some(Duration::from_millis(250))]);
}

#[test]
fn live_status_revisions_coalesce_into_one_background_snapshot_refresh() {
    let events = Arc::new(FakeEvents::default());
    for upload in [20, 30, 40] {
        let mut changed = status(upload);
        changed.runtime_generation = Some(ratash::domain::RuntimeGeneration(upload));
        events.push(StatusLogEvent::Status {
            connection_generation: 1,
            status: Box::new(changed),
        });
    }
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect = PassiveReconnect;
    let mut input = ScriptedInput::quit_on_poll(4);
    let clock = AdjustableClock::new(Duration::from_secs(1), 10_000);
    let waiter = AdvancingWaiter::new(clock.clone());
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();
    let waker = RuntimeWaker::default();

    let mut runtime = StatusInterfaceRuntime::new(
        1,
        snapshot(10),
        StatusInterfacePorts {
            events,
            dispatcher: &mut dispatcher,
            reconnect: &mut reconnect,
            input: &mut input,
            waiter: &waiter,
            waker,
            clock: &clock,
            signal: &signal,
            renderer: &mut renderer,
        },
    );
    runtime
        .run()
        .expect("coalesced snapshot refresh should remain live");
    drop(runtime);

    assert!(matches!(
        dispatcher.submitted.as_slice(),
        [Command::RefreshSnapshot {
            connection_generation: 1,
            ..
        }]
    ));
    assert_eq!(waiter.waits(), vec![Some(Duration::from_millis(100))]);
}

#[test]
fn profile_refresh_deadline_requests_a_bounded_full_snapshot() {
    let events = Arc::new(FakeEvents::default());
    let mut initial = snapshot(10);
    initial.profiles.push(profile_row(10_000));
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect = PassiveReconnect;
    let mut input = ScriptedInput::quit_on_poll(3);
    let clock = AdjustableClock::new(Duration::ZERO, 10_000);
    let waiter = AdvancingWaiter::new(clock.clone());
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();
    let waker = RuntimeWaker::default();

    let mut runtime = StatusInterfaceRuntime::new(
        1,
        initial,
        StatusInterfacePorts {
            events,
            dispatcher: &mut dispatcher,
            reconnect: &mut reconnect,
            input: &mut input,
            waiter: &waiter,
            waker,
            clock: &clock,
            signal: &signal,
            renderer: &mut renderer,
        },
    );
    runtime
        .run()
        .expect("scheduled Profile freshness should remain bounded");
    drop(runtime);

    assert!(matches!(
        dispatcher.submitted.as_slice(),
        [Command::RefreshSnapshot {
            connection_generation: 1,
            ..
        }]
    ));
    assert_eq!(waiter.waits(), vec![Some(Duration::from_secs(1))]);
}

#[test]
fn background_snapshot_refresh_replaces_profiles_and_proxies() {
    let events = Arc::new(FakeEvents::default());
    let mut refreshed = snapshot(90);
    refreshed.profiles.push(profile_row(40_000));
    refreshed.proxies.push(ProxyRow {
        group_id: ProxyGroupId::for_name("Automatic"),
        group: "Automatic".to_owned(),
        node_id: Some(NodeRecordId::for_core("Berlin")),
        name: "Berlin".to_owned(),
        node_type: "ss".to_owned(),
        available: true,
        selected: true,
        delay_ms: Some(24),
        sampled_at_unix_ms: Some(30_000),
        freshness: LatencyFreshness::Fresh,
        probe_status: LatencyProbeStatus::Succeeded,
    });
    let mut dispatcher = RecordingDispatcher::default();
    dispatcher.results.push_back(DispatchedEvent {
        source: ratash::tui::EventSource::CommandResult,
        event: UiEvent::SnapshotRefreshed {
            connection_generation: 1,
            base_view_revision: 2,
            base_status_revision: 2,
            snapshot: refreshed,
        },
    });
    let mut reconnect = PassiveReconnect;
    let mut input = ScriptedInput::quit_on_poll(2);
    let clock = FixedClock(Duration::ZERO);
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();
    let waker = RuntimeWaker::default();

    let mut runtime = StatusInterfaceRuntime::new(
        1,
        snapshot(10),
        StatusInterfacePorts {
            events,
            dispatcher: &mut dispatcher,
            reconnect: &mut reconnect,
            input: &mut input,
            waiter: &waker,
            waker: waker.clone(),
            clock: &clock,
            signal: &signal,
            renderer: &mut renderer,
        },
    );
    runtime
        .run()
        .expect("background snapshot should update the visible collections");

    assert_eq!(runtime.state().profiles.rows.len(), 1);
    assert_eq!(runtime.state().proxies.rows.len(), 1);
    assert_eq!(runtime.state().proxies.rows[0].delay_ms, Some(24));
}

#[test]
fn mutation_resync_completes_during_live_traffic_without_rolling_status_back() {
    let profile_id = ProfileId::new();
    let mut initial = snapshot(10);
    initial.profiles.push(ProfileRow {
        id: profile_id,
        name: "Initial".to_owned(),
        active: false,
        fresh: true,
        last_success_at_unix_ms: 1,
        next_refresh_at_unix_ms: 40_000,
        error: None,
    });
    let mut refreshed = initial.clone();
    refreshed.status.traffic.upload_bytes_per_second = 25;
    refreshed.profiles[0].active = true;
    refreshed.profiles[0].name = "Resynced".to_owned();
    let events = Arc::new(ScriptedEvents::new([
        Some(StatusLogEvent::Status {
            connection_generation: 1,
            status: Box::new(status_with_upload(&initial.status, 20)),
        }),
        None,
        Some(StatusLogEvent::Status {
            connection_generation: 1,
            status: Box::new(status_with_upload(&initial.status, 30)),
        }),
        None,
        None,
        Some(StatusLogEvent::Status {
            connection_generation: 1,
            status: Box::new(status_with_upload(&initial.status, 40)),
        }),
        None,
    ]));
    let mut dispatcher = MutationResyncDispatcher::new(refreshed);
    let mut reconnect = PassiveReconnect;
    let mut input = ActivationThenQuitInput {
        polls: 0,
        quit_on: 7,
        profile_id,
    };
    let clock = AdjustableClock::new(Duration::ZERO, 10_000);
    let waiter = AdvancingWaiter::new(clock.clone());
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();
    let waker = RuntimeWaker::default();

    let mut runtime = StatusInterfaceRuntime::new(
        1,
        initial,
        StatusInterfacePorts {
            events,
            dispatcher: &mut dispatcher,
            reconnect: &mut reconnect,
            input: &mut input,
            waiter: &waiter,
            waker,
            clock: &clock,
            signal: &signal,
            renderer: &mut renderer,
        },
    );
    runtime.run().expect("mutation resync should remain live");

    assert_eq!(runtime.state().profiles.rows[0].name, "Resynced");
    assert!(!runtime.state().connection.snapshot_stale);
    assert_eq!(
        runtime
            .state()
            .status
            .as_ref()
            .expect("latest live status should remain visible")
            .traffic
            .upload_bytes_per_second,
        40
    );
    assert_eq!(runtime.state().toast.as_deref(), Some("Success: done"));
    drop(runtime);
    assert!(matches!(
        dispatcher.submitted.as_slice(),
        [
            Command::ActivateProfile { .. },
            Command::RefreshSnapshot { .. }
        ]
    ));
}

#[test]
fn identical_background_snapshot_does_not_render_an_extra_frame() {
    let events = Arc::new(FakeEvents::default());
    let initial = snapshot(10);
    let mut dispatcher = RecordingDispatcher::default();
    dispatcher.results.push_back(DispatchedEvent {
        source: ratash::tui::EventSource::CommandResult,
        event: UiEvent::SnapshotRefreshed {
            connection_generation: 1,
            base_view_revision: 2,
            base_status_revision: 2,
            snapshot: initial.clone(),
        },
    });
    let mut reconnect = PassiveReconnect;
    let mut input = ScriptedInput::quit_on_poll(2);
    let clock = FixedClock(Duration::ZERO);
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();
    let waker = RuntimeWaker::default();

    let mut runtime = StatusInterfaceRuntime::new(
        1,
        initial,
        StatusInterfacePorts {
            events,
            dispatcher: &mut dispatcher,
            reconnect: &mut reconnect,
            input: &mut input,
            waiter: &waker,
            waker: waker.clone(),
            clock: &clock,
            signal: &signal,
            renderer: &mut renderer,
        },
    );
    runtime
        .run()
        .expect("identical background snapshot should remain clean");
    drop(runtime);

    assert_eq!(renderer.uploads, vec![10]);
}

#[test]
fn reconnect_deadline_dispatches_the_next_connection_generation() {
    let events = Arc::new(FakeEvents::default());
    events.push(StatusLogEvent::Disconnected {
        connection_generation: 4,
    });
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect = ImmediateReconnect::default();
    let mut input = ScriptedInput::quit_on_poll(3);
    let clock = FixedClock(Duration::from_secs(5));
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();
    let waker = RuntimeWaker::default();

    {
        let mut runtime = StatusInterfaceRuntime::new(
            4,
            snapshot(1),
            StatusInterfacePorts {
                events,
                dispatcher: &mut dispatcher,
                reconnect: &mut reconnect,
                input: &mut input,
                waiter: &waker,
                waker: waker.clone(),
                clock: &clock,
                signal: &signal,
                renderer: &mut renderer,
            },
        );
        runtime
            .run()
            .expect("scripted reconnect should remain bounded");
    }

    assert!(matches!(
        dispatcher.submitted.as_slice(),
        [Command::Connect {
            connection_generation: 5
        }]
    ));
}

#[test]
fn live_log_batches_keep_only_the_bounded_tail() {
    let events = Arc::new(FakeEvents::default());
    events.push(StatusLogEvent::Logs {
        connection_generation: 1,
        records: (0..LOG_CAPACITY + 7)
            .map(|sequence| ViewLogRecord {
                sequence: sequence as u64,
                timestamp_unix_ms: sequence as u64,
                level: ratash::telemetry::LogLevel::Info,
                source: ratash::telemetry::LogSource::CoreApi,
                message: "fixture".to_owned(),
            })
            .collect(),
        gap: false,
        dropped_total: 0,
    });
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect = PassiveReconnect;
    let mut input = ScriptedInput::quit_on_poll(2);
    let clock = FixedClock(Duration::ZERO);
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();
    let waker = RuntimeWaker::default();

    {
        let mut runtime = StatusInterfaceRuntime::new(
            1,
            snapshot(0),
            StatusInterfacePorts {
                events,
                dispatcher: &mut dispatcher,
                reconnect: &mut reconnect,
                input: &mut input,
                waiter: &waker,
                waker: waker.clone(),
                clock: &clock,
                signal: &signal,
                renderer: &mut renderer,
            },
        );
        runtime
            .run()
            .expect("bounded log intake should remain live");
        assert_eq!(runtime.state().logs.records.len(), LOG_CAPACITY);
        assert_eq!(
            runtime
                .state()
                .logs
                .records
                .front()
                .expect("bounded tail should retain records")
                .sequence,
            7
        );
    }
}

#[test]
fn background_dispatcher_runs_commands_off_the_render_thread_and_cancels_results() {
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
    let commands = Arc::new(BlockingCommands {
        started: started_sender,
        release: Mutex::new(release_receiver),
        finished: finished_sender,
    });
    let events = Arc::new(FakeEvents::default());
    let sources = StatusInterfaceSources {
        snapshots: Arc::new(FakeSnapshots::successful(snapshot(0))),
        events,
        commands,
    };
    let mut dispatcher =
        BackgroundCommandDispatcher::new(sources).expect("fixture command workers should start");
    let request_id = RequestId(91);
    let profile_id = ProfileId::new();

    dispatcher
        .submit(Command::ActivateProfile {
            request_id,
            connection_generation: 2,
            profile_id,
        })
        .expect("fixture command should enter the bounded queue");
    let (worker_thread, operation) = started_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("worker should begin the command");
    assert_ne!(worker_thread, thread::current().id());
    assert_eq!(
        operation,
        ApplicationOperation::ProfileUse {
            profile: profile_id.to_string()
        }
    );
    dispatcher.cancel(request_id);
    release_sender
        .send(())
        .expect("fixture command should be released");
    finished_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("fixture command should finish");

    assert!(
        dispatcher
            .try_next()
            .expect("result queue should remain open")
            .is_none()
    );
    dispatcher.shutdown();
}

#[test]
fn mutation_dispatcher_cancels_the_active_wait_and_keeps_only_the_latest_pending_intent() {
    let (started_sender, started_receiver) = mpsc::sync_channel(3);
    let (cancelled_sender, cancelled_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let sources = StatusInterfaceSources {
        snapshots: Arc::new(FakeSnapshots::successful(snapshot(0))),
        events: Arc::new(FakeEvents::default()),
        commands: Arc::new(CoalescingCommands {
            started: started_sender,
            cancelled: cancelled_sender,
            release: Mutex::new(release_receiver),
        }),
    };
    let mut dispatcher =
        BackgroundCommandDispatcher::new(sources).expect("fixture command workers should start");
    let waker = RuntimeWaker::default();
    dispatcher.install_waker(waker.clone());
    let profile_id = ProfileId::new();
    dispatcher
        .submit(Command::ActivateProfile {
            request_id: RequestId(101),
            connection_generation: 1,
            profile_id,
        })
        .expect("first mutation should start");
    assert!(matches!(
        started_receiver.recv_timeout(Duration::from_secs(1)),
        Ok(ApplicationOperation::ProfileUse { .. })
    ));

    dispatcher
        .submit(Command::SelectNode {
            request_id: RequestId(102),
            connection_generation: 1,
            group_id: ProxyGroupId::for_name("Automatic"),
            node_id: NodeRecordId::for_core("Berlin"),
        })
        .expect("second mutation should become pending");
    cancelled_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("supersession should interrupt the active wait");
    dispatcher
        .submit(Command::SelectNode {
            request_id: RequestId(103),
            connection_generation: 1,
            group_id: ProxyGroupId::for_name("Automatic"),
            node_id: NodeRecordId::for_core("Paris"),
        })
        .expect("latest mutation should replace the pending mutation");

    let checkpoint = waker.checkpoint();
    release_sender
        .send(())
        .expect("active cancelled operation should finish");
    assert_eq!(
        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("latest mutation should be promoted"),
        ApplicationOperation::ProxySelect {
            group: ProxyGroupId::for_name("Automatic").as_str().to_owned(),
            node: NodeRecordId::for_core("Paris").as_str().to_owned(),
        }
    );
    waker.wait(checkpoint, Some(Duration::from_secs(1)));
    assert!(matches!(
        dispatcher
            .try_next()
            .expect("result queue should remain open")
            .expect("latest mutation should publish its acknowledgement")
            .event,
        UiEvent::CommandResult {
            request_id: RequestId(103),
            result: Ok(_),
            ..
        }
    ));
    assert!(
        started_receiver
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    dispatcher.shutdown();
}

#[test]
fn successful_mutation_dispatches_an_ack_without_waiting_for_resync() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let sources = StatusInterfaceSources {
        snapshots: Arc::new(FakeSnapshots {
            snapshot: snapshot(73),
            order: Arc::clone(&order),
            fail: false,
        }),
        events: Arc::new(FakeEvents::default()),
        commands: Arc::new(OrderedCommands {
            order: Arc::clone(&order),
        }),
    };
    let mut dispatcher =
        BackgroundCommandDispatcher::new(sources).expect("fixture command workers should start");
    let waker = RuntimeWaker::default();
    dispatcher.install_waker(waker.clone());
    let checkpoint = waker.checkpoint();

    dispatcher
        .submit(Command::SelectNode {
            request_id: RequestId(92),
            connection_generation: 6,
            group_id: ProxyGroupId::for_name("Automatic"),
            node_id: NodeRecordId::for_core("Berlin"),
        })
        .expect("fixture command should enter the bounded queue");
    waker.wait(checkpoint, Some(Duration::from_secs(1)));
    let dispatched = dispatcher
        .try_next()
        .expect("result queue should remain open")
        .expect("successful mutation should dispatch a bounded result");

    match dispatched.event {
        UiEvent::CommandResult {
            request_id,
            connection_generation,
            result: Ok(success),
        } => {
            assert_eq!(request_id, RequestId(92));
            assert_eq!(connection_generation, 6);
            assert_eq!(success.message, "done");
        }
        other => panic!("unexpected mutation event: {other:?}"),
    }
    assert_eq!(
        order
            .lock()
            .expect("order lock should be available")
            .as_slice(),
        ["command"]
    );
    dispatcher.shutdown();
}

#[test]
fn idle_background_command_workers_block_until_work_or_shutdown() {
    let sources = StatusInterfaceSources {
        snapshots: Arc::new(FakeSnapshots::successful(snapshot(0))),
        events: Arc::new(FakeEvents::default()),
        commands: Arc::new(ImmediateCommands),
    };
    let mut dispatcher =
        BackgroundCommandDispatcher::new(sources).expect("fixture command workers should start");
    let waker = RuntimeWaker::default();
    dispatcher.install_waker(waker.clone());
    let checkpoint = waker.checkpoint();

    dispatcher
        .submit(Command::FetchProxyGroup {
            request_id: RequestId(94),
            connection_generation: 6,
            group_id: ProxyGroupId::for_name("Automatic"),
        })
        .expect("fixture work should enter the bounded queue");
    waker.wait(checkpoint, Some(Duration::from_secs(1)));
    dispatcher
        .try_next()
        .expect("result queue should remain open")
        .expect("fixture work should complete");
    let completed_waits = dispatcher.worker_wait_return_count();
    assert!(completed_waits >= 1);

    thread::sleep(Duration::from_millis(100));

    assert_eq!(dispatcher.worker_wait_return_count(), completed_waits);
    dispatcher.shutdown();
}

#[test]
fn background_dispatcher_loads_one_proxy_group_without_a_full_snapshot() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut view = snapshot(0);
    view.proxy_groups = vec![ProxyGroupRow {
        id: ProxyGroupId::for_name("Manual"),
        name: "Manual".to_owned(),
        proxy_type: "Selector".to_owned(),
        selected_node: Some("Paris".to_owned()),
    }];
    let sources = StatusInterfaceSources {
        snapshots: Arc::new(FakeSnapshots {
            snapshot: view,
            order: Arc::clone(&order),
            fail: false,
        }),
        events: Arc::new(FakeEvents::default()),
        commands: Arc::new(ImmediateCommands),
    };
    let mut dispatcher =
        BackgroundCommandDispatcher::new(sources).expect("fixture command workers should start");
    let waker = RuntimeWaker::default();
    dispatcher.install_waker(waker.clone());
    let checkpoint = waker.checkpoint();

    dispatcher
        .submit(Command::FetchProxyGroup {
            request_id: RequestId(93),
            connection_generation: 6,
            group_id: ProxyGroupId::for_name("Manual"),
        })
        .expect("Proxy Group request should enter the bounded queue");
    waker.wait(checkpoint, Some(Duration::from_secs(1)));

    assert!(matches!(
        dispatcher
            .try_next()
            .expect("result queue should remain open")
            .expect("Proxy Group result should be published")
            .event,
        UiEvent::ProxyGroupLoaded {
            request_id: RequestId(93),
            connection_generation: 6,
            result: Ok(ProxyGroupSnapshot { group, .. }),
        } if group.name == "Manual"
    ));
    assert_eq!(
        order
            .lock()
            .expect("order lock should be available")
            .as_slice(),
        ["group"]
    );
    dispatcher.shutdown();
}

#[test]
fn background_dispatcher_preserves_rule_request_identity() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let sources = StatusInterfaceSources {
        snapshots: Arc::new(FakeSnapshots {
            snapshot: snapshot(0),
            order: Arc::clone(&order),
            fail: false,
        }),
        events: Arc::new(FakeEvents::default()),
        commands: Arc::new(ImmediateCommands),
    };
    let mut dispatcher =
        BackgroundCommandDispatcher::new(sources).expect("fixture command workers should start");
    let waker = RuntimeWaker::default();
    dispatcher.install_waker(waker.clone());
    let checkpoint = waker.checkpoint();

    dispatcher
        .submit(Command::FetchRules {
            request_id: RequestId(95),
            connection_generation: 6,
        })
        .expect("Rule request should enter the bounded queue");
    waker.wait(checkpoint, Some(Duration::from_secs(1)));

    assert!(matches!(
        dispatcher
            .try_next()
            .expect("result queue should remain open")
            .expect("Rule result should be published")
            .event,
        UiEvent::RulesLoaded {
            request_id: RequestId(95),
            connection_generation: 6,
            result: Ok(RuleListSnapshot {
                initialized: true,
                ..
            }),
        }
    ));
    assert_eq!(
        order
            .lock()
            .expect("order lock should be available")
            .as_slice(),
        ["rules"]
    );
    dispatcher.shutdown();
}

#[test]
fn background_dispatcher_maps_mutations_to_application_operations() {
    let operations = Arc::new(Mutex::new(Vec::new()));
    let sources = StatusInterfaceSources {
        snapshots: Arc::new(FakeSnapshots::successful(snapshot(0))),
        events: Arc::new(FakeEvents::default()),
        commands: Arc::new(RecordingRuleCommands {
            operations: Arc::clone(&operations),
        }),
    };
    let mut dispatcher =
        BackgroundCommandDispatcher::new(sources).expect("fixture command workers should start");
    let waker = RuntimeWaker::default();
    dispatcher.install_waker(waker.clone());
    let commands = [
        Command::AddRule {
            request_id: RequestId(201),
            connection_generation: 6,
            rule: "DOMAIN,one.example,DIRECT".to_owned(),
        },
        Command::ReplaceRule {
            request_id: RequestId(202),
            connection_generation: 6,
            old_rule: "DOMAIN,one.example,DIRECT".to_owned(),
            new_rule: "DOMAIN,two.example,DIRECT".to_owned(),
        },
        Command::RemoveRule {
            request_id: RequestId(203),
            connection_generation: 6,
            rule: "DOMAIN,two.example,DIRECT".to_owned(),
        },
        Command::RestartSupervisor {
            request_id: RequestId(204),
            connection_generation: 6,
        },
        Command::StopSupervisor {
            request_id: RequestId(205),
            connection_generation: 6,
        },
    ];

    for command in commands {
        let checkpoint = waker.checkpoint();
        dispatcher
            .submit(command)
            .expect("Rule mutation should enter the bounded queue");
        waker.wait(checkpoint, Some(Duration::from_secs(1)));
        assert!(matches!(
            dispatcher
                .try_next()
                .expect("result queue should remain open")
                .expect("Rule mutation should publish an acknowledgement")
                .event,
            UiEvent::CommandResult { result: Ok(_), .. }
        ));
    }

    assert_eq!(
        operations
            .lock()
            .expect("operation lock should be available")
            .as_slice(),
        [
            ApplicationOperation::RuleAdd {
                rule: "DOMAIN,one.example,DIRECT".to_owned(),
                placement: RulePlacement::Append,
            },
            ApplicationOperation::RuleReplace {
                old_rule: "DOMAIN,one.example,DIRECT".to_owned(),
                new_rule: "DOMAIN,two.example,DIRECT".to_owned(),
            },
            ApplicationOperation::RuleRemove {
                rule: "DOMAIN,two.example,DIRECT".to_owned(),
            },
            ApplicationOperation::Restart,
            ApplicationOperation::Stop,
        ]
    );
    dispatcher.shutdown();
}

#[test]
fn application_command_executor_accepts_rule_and_lifecycle_outcomes() {
    let executor = ApplicationCommandExecutor::new(Arc::new(MutationOutputClient));
    let cancellation = CancellationToken::default();

    assert_eq!(
        executor
            .execute(
                ApplicationOperation::RuleAdd {
                    rule: "DOMAIN,example.com,DIRECT".to_owned(),
                    placement: RulePlacement::Append,
                },
                &cancellation,
            )
            .expect("Rule mutation output should be accepted"),
        "Rule added"
    );
    assert_eq!(
        executor
            .execute(ApplicationOperation::Restart, &cancellation)
            .expect("lifecycle output should be accepted"),
        "Supervisor restarted"
    );
}

#[test]
fn shutdown_signal_exits_and_restores_every_terminal_mode() {
    let events = Arc::new(FakeEvents::default());
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect = PassiveReconnect;
    let mut input = ScriptedInput::never();
    let clock = FixedClock(Duration::ZERO);
    let signal = ImmediateShutdown;
    let mut renderer = RecordingRenderer::new();
    let mut terminal = RecordingTerminal::default();
    let waker = RuntimeWaker::default();

    run_with_terminal_session(&mut terminal, || {
        let mut runtime = StatusInterfaceRuntime::new(
            1,
            snapshot(0),
            StatusInterfacePorts {
                events,
                dispatcher: &mut dispatcher,
                reconnect: &mut reconnect,
                input: &mut input,
                waiter: &waker,
                waker: waker.clone(),
                clock: &clock,
                signal: &signal,
                renderer: &mut renderer,
            },
        );
        runtime.run()
    })
    .expect("shutdown signal should produce a clean exit");

    assert_eq!(
        terminal.actions.last(),
        Some(&TerminalAction::DisableRawMode)
    );
    assert!(terminal.actions.contains(&TerminalAction::ShowCursor));
    assert!(
        terminal
            .actions
            .contains(&TerminalAction::LeaveAlternateScreen)
    );
}

#[test]
fn terminal_session_restores_modes_after_errors_and_panics() {
    let mut error_terminal = RecordingTerminal::default();
    let error = run_with_terminal_session(&mut error_terminal, || -> Result<(), _> {
        Err(StatusInterfaceError::new(
            StatusInterfaceErrorKind::Render,
            "injected render failure",
        ))
    })
    .expect_err("injected runtime error should be returned");
    assert_eq!(error.kind, StatusInterfaceErrorKind::Render);
    assert_eq!(
        error_terminal.actions.last(),
        Some(&TerminalAction::DisableRawMode)
    );

    let mut panic_terminal = RecordingTerminal::default();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = run_with_terminal_session(&mut panic_terminal, || -> Result<(), _> {
            panic!("injected runtime panic");
        });
    }));
    assert!(result.is_err());
    assert_eq!(
        panic_terminal.actions.last(),
        Some(&TerminalAction::DisableRawMode)
    );
}

fn snapshot(upload: u64) -> FullViewSnapshot {
    FullViewSnapshot {
        status: status(upload),
        proxy_groups: Vec::new(),
        proxies: Vec::new(),
        profiles: Vec::new(),
        logs: Vec::new(),
        dropped_logs: 0,
    }
}

fn profile_row(next_refresh_at_unix_ms: u64) -> ProfileRow {
    ProfileRow {
        id: ProfileId::new(),
        name: "Fixture".to_owned(),
        active: true,
        fresh: true,
        last_success_at_unix_ms: 1,
        next_refresh_at_unix_ms,
        error: None,
    }
}

fn status(upload: u64) -> StatusSnapshot {
    StatusSnapshot {
        supervisor: SupervisorStatus {
            lifecycle: SupervisorLifecycle::Ready,
            started_at_unix_ms: 1,
            uptime_seconds: 2,
            health_reasons: Vec::new(),
        },
        core: CoreStatus {
            lifecycle: CoreLifecycle::Ready,
            pid: Some(42),
            instance_generation: None,
            restart: ratash::domain::CoreRestartStatus::default(),
        },
        tun: TunStatus {
            requested: true,
            capable: true,
            effective: true,
            reason: None,
        },
        active_profile: Some(ActiveProfileSummary {
            id: ProfileId::new(),
            name: "Fixture".to_owned(),
        }),
        primary_proxy_group: None,
        selected_node: None,
        latency: None,
        traffic: TrafficSample {
            upload_bytes_per_second: upload,
            download_bytes_per_second: upload.saturating_mul(2),
            sampled_at_unix_ms: Some(3),
            state: SampleState::Fresh,
        },
        connection_count: 0,
        runtime_generation: None,
        apply_state: ApplyState::Idle,
        runtime_apply: RuntimeApplySnapshot::default(),
        selection_restore_pending: false,
        probe_queue: ProbeQueueStatus::default(),
        stream_health: StreamHealthSet {
            traffic: StreamState::Healthy,
            connections: StreamState::Healthy,
            logs: StreamState::Healthy,
        },
    }
}

fn status_with_upload(status: &StatusSnapshot, upload: u64) -> StatusSnapshot {
    let mut status = status.clone();
    status.traffic.upload_bytes_per_second = upload;
    status.traffic.download_bytes_per_second = upload.saturating_mul(2);
    status
}

struct FakeSnapshots {
    snapshot: FullViewSnapshot,
    order: Arc<Mutex<Vec<&'static str>>>,
    fail: bool,
}

impl FakeSnapshots {
    fn successful(snapshot: FullViewSnapshot) -> Self {
        Self {
            snapshot,
            order: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        }
    }
}

impl FullSnapshotSource for FakeSnapshots {
    fn fetch_full_snapshot(
        &self,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<FullViewSnapshot, StatusInterfaceError> {
        self.order
            .lock()
            .expect("order lock should be available")
            .push("snapshot");
        if self.fail {
            Err(StatusInterfaceError::new(
                StatusInterfaceErrorKind::Snapshot,
                "injected snapshot failure",
            ))
        } else {
            Ok(self.snapshot.clone())
        }
    }

    fn fetch_proxy_group(
        &self,
        group: &str,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<ProxyGroupSnapshot, StatusInterfaceError> {
        self.order
            .lock()
            .expect("order lock should be available")
            .push("group");
        let group_row = self
            .snapshot
            .proxy_groups
            .iter()
            .find(|candidate| candidate.name == group || candidate.id.as_str() == group)
            .cloned()
            .unwrap_or_else(|| ProxyGroupRow {
                id: ProxyGroupId::parse(group).unwrap_or_else(|_| ProxyGroupId::for_name(group)),
                name: group.to_owned(),
                proxy_type: "Selector".to_owned(),
                selected_node: None,
            });
        let groups = if self.snapshot.proxy_groups.is_empty() {
            vec![group_row.clone()]
        } else {
            self.snapshot.proxy_groups.clone()
        };
        Ok(ProxyGroupSnapshot {
            group: group_row,
            groups,
            proxies: self.snapshot.proxies.clone(),
        })
    }

    fn fetch_rules(
        &self,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<RuleListSnapshot, StatusInterfaceError> {
        self.order
            .lock()
            .expect("order lock should be available")
            .push("rules");
        Ok(RuleListSnapshot {
            initialized: true,
            revision: Some(LocalRuleSetRevision(3)),
            rows: vec![RuleRow {
                index: 0,
                rule_string: "MATCH,DIRECT".to_owned(),
                rule_type: "MATCH".to_owned(),
                payload: None,
                policy_target: "DIRECT".to_owned(),
                policy_target_validation: PolicyTargetValidation::Valid,
            }],
        })
    }
}

#[derive(Default)]
struct FakeEvents {
    order: Option<Arc<Mutex<Vec<&'static str>>>>,
    events: Mutex<VecDeque<StatusLogEvent>>,
    disconnected: Mutex<Vec<u64>>,
    tail_requests: Mutex<Vec<Option<u64>>>,
}

impl FakeEvents {
    fn with_order(order: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            order: Some(order),
            ..Self::default()
        }
    }

    fn push(&self, event: StatusLogEvent) {
        self.events
            .lock()
            .expect("event lock should be available")
            .push_back(event);
    }

    fn disconnects(&self) -> Vec<u64> {
        self.disconnected
            .lock()
            .expect("disconnect lock should be available")
            .clone()
    }

    fn tail_requests(&self) -> Vec<Option<u64>> {
        self.tail_requests
            .lock()
            .expect("tail request lock should be available")
            .clone()
    }
}

impl StatusLogEventSource for FakeEvents {
    fn connect(
        &self,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<(), StatusInterfaceError> {
        if let Some(order) = &self.order {
            order
                .lock()
                .expect("order lock should be available")
                .push("connect");
        }
        Ok(())
    }

    fn try_next(&self) -> Result<Option<StatusLogEvent>, StatusInterfaceError> {
        Ok(self
            .events
            .lock()
            .expect("event lock should be available")
            .pop_front())
    }

    fn fetch_log_tail(
        &self,
        _connection_generation: u64,
        after_sequence: Option<u64>,
        _cancellation: &CancellationToken,
    ) -> Result<LogTail, StatusInterfaceError> {
        self.tail_requests
            .lock()
            .expect("tail request lock should be available")
            .push(after_sequence);
        Ok(LogTail {
            records: Vec::new(),
            gap: false,
            dropped_total: 0,
        })
    }

    fn disconnect(&self, connection_generation: u64) {
        self.disconnected
            .lock()
            .expect("disconnect lock should be available")
            .push(connection_generation);
    }
}

struct ScriptedEvents {
    events: Mutex<VecDeque<Option<StatusLogEvent>>>,
}

impl ScriptedEvents {
    fn new(events: impl IntoIterator<Item = Option<StatusLogEvent>>) -> Self {
        Self {
            events: Mutex::new(events.into_iter().collect()),
        }
    }
}

impl StatusLogEventSource for ScriptedEvents {
    fn connect(
        &self,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<(), StatusInterfaceError> {
        Ok(())
    }

    fn try_next(&self) -> Result<Option<StatusLogEvent>, StatusInterfaceError> {
        Ok(self
            .events
            .lock()
            .expect("scripted event lock should be available")
            .pop_front()
            .flatten())
    }

    fn fetch_log_tail(
        &self,
        _connection_generation: u64,
        _after_sequence: Option<u64>,
        _cancellation: &CancellationToken,
    ) -> Result<LogTail, StatusInterfaceError> {
        Ok(LogTail {
            records: Vec::new(),
            gap: false,
            dropped_total: 0,
        })
    }

    fn disconnect(&self, _connection_generation: u64) {}
}

struct ImmediateCommands;

impl UiCommandExecutor for ImmediateCommands {
    fn execute(
        &self,
        _operation: ApplicationOperation,
        _cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError> {
        Ok("done".to_owned())
    }
}

struct MutationOutputClient;

impl ApplicationClient for MutationOutputClient {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        match operation {
            ApplicationOperation::RuleAdd { rule, .. } => {
                Ok(ApplicationOutput::RuleMutation(RuleMutationOutcome {
                    action: RuleMutationAction::Added,
                    changed_rule: rule,
                    previous_rule: None,
                    resulting_position: Some(0),
                    runtime_apply: RuntimeApplyOutcome {
                        status: RuntimeApplyStatus::Applied,
                        candidate_generation: Some(ratash::domain::RuntimeGeneration(2)),
                        committed_generation: Some(ratash::domain::RuntimeGeneration(2)),
                        recovery: RecoveryOutcome {
                            status: RecoveryStatus::NotRequired,
                            restored_generation: None,
                            message: None,
                        },
                    },
                }))
            }
            ApplicationOperation::Restart => Ok(ApplicationOutput::Lifecycle(LifecycleOutcome {
                action: LifecycleAction::Restart,
                changed: true,
                status: status(55),
            })),
            _ => panic!("unexpected fixture operation"),
        }
    }
}

struct RecordingRuleCommands {
    operations: Arc<Mutex<Vec<ApplicationOperation>>>,
}

impl UiCommandExecutor for RecordingRuleCommands {
    fn execute(
        &self,
        operation: ApplicationOperation,
        _cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError> {
        self.operations
            .lock()
            .expect("operation lock should be available")
            .push(operation);
        Ok("done".to_owned())
    }
}

struct OrderedCommands {
    order: Arc<Mutex<Vec<&'static str>>>,
}

impl UiCommandExecutor for OrderedCommands {
    fn execute(
        &self,
        _operation: ApplicationOperation,
        _cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError> {
        self.order
            .lock()
            .expect("order lock should be available")
            .push("command");
        Ok("done".to_owned())
    }
}

#[derive(Default)]
struct SnapshotClient {
    operations: Mutex<Vec<ApplicationOperation>>,
}

impl ApplicationClient for SnapshotClient {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        self.operations
            .lock()
            .expect("operation lock should be available")
            .push(operation.clone());
        match operation {
            ApplicationOperation::GetStatus => Ok(ApplicationOutput::Status(status(55))),
            ApplicationOperation::ProfileList => {
                Ok(ApplicationOutput::Profiles(ProfileListOutcome {
                    profiles: Vec::new(),
                }))
            }
            ApplicationOperation::RuleList => Ok(ApplicationOutput::Rules(RuleListOutcome {
                initialized: true,
                revision: Some(LocalRuleSetRevision(9)),
                rules: vec![RuleSummary {
                    index: 0,
                    rule_string: "DOMAIN-SUFFIX,example.com,PROXY".to_owned(),
                    rule_type: "DOMAIN-SUFFIX".to_owned(),
                    payload: Some("example.com".to_owned()),
                    policy_target: "PROXY".to_owned(),
                    params: Vec::new(),
                    policy_target_validation: PolicyTargetValidation::Valid,
                }],
            })),
            _ => panic!("snapshot fixture received an unexpected operation"),
        }
    }
}

#[derive(Default)]
struct ProxySnapshotClient {
    operations: Mutex<Vec<ApplicationOperation>>,
}

impl ApplicationClient for ProxySnapshotClient {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        self.operations
            .lock()
            .expect("operation lock should be available")
            .push(operation.clone());
        match operation {
            ApplicationOperation::GetStatus => {
                let mut snapshot = status(55);
                snapshot.primary_proxy_group = Some("Automatic".to_owned());
                Ok(ApplicationOutput::Status(snapshot))
            }
            ApplicationOperation::ProfileList => {
                Ok(ApplicationOutput::Profiles(ProfileListOutcome {
                    profiles: Vec::new(),
                }))
            }
            ApplicationOperation::ProxyList { group } => {
                let node_name =
                    if group == "Manual" || group == ProxyGroupId::for_name("Manual").as_str() {
                        "Paris"
                    } else {
                        "Tokyo"
                    };
                Ok(ApplicationOutput::Proxies(ProxyListOutcome {
                    group: proxy_group_summary(&group, node_name),
                    groups: vec![
                        proxy_group_summary("Automatic", "Tokyo"),
                        proxy_group_summary("Manual", "Paris"),
                    ],
                    nodes: vec![ProxyNodeRow {
                        id: Some(NodeRecordId::for_core(node_name)),
                        name: node_name.to_owned(),
                        member_kind: ProxyMemberKind::Node,
                        source: None,
                        candidate_ids: Vec::new(),
                        proxy_type: Some("Shadowsocks".to_owned()),
                        availability: ProxyAvailability::Available,
                        selected: true,
                        delay_ms: Some(21),
                        sampled_at_unix_ms: Some(2_000),
                        freshness: LatencyFreshness::Fresh,
                        probe_status: LatencyProbeStatus::Succeeded,
                    }],
                }))
            }
            _ => panic!("Proxy snapshot fixture received an unexpected operation"),
        }
    }
}

fn proxy_group_summary(name: &str, selected_node: &str) -> ProxyGroupSummary {
    ProxyGroupSummary {
        id: ProxyGroupId::for_name(name),
        name: name.to_owned(),
        proxy_type: "Selector".to_owned(),
        selectable: true,
        selected_node: Some(ratash::application::SelectorIdentity {
            id: NodeRecordId::for_core(selected_node).as_str().to_owned(),
            name: selected_node.to_owned(),
        }),
    }
}

struct BlockingCommands {
    started: mpsc::SyncSender<(thread::ThreadId, ApplicationOperation)>,
    release: Mutex<mpsc::Receiver<()>>,
    finished: mpsc::SyncSender<()>,
}

struct CoalescingCommands {
    started: mpsc::SyncSender<ApplicationOperation>,
    cancelled: mpsc::SyncSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl UiCommandExecutor for CoalescingCommands {
    fn execute(
        &self,
        operation: ApplicationOperation,
        cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError> {
        self.started
            .send(operation.clone())
            .expect("test should receive each started mutation");
        if matches!(operation, ApplicationOperation::ProfileUse { .. }) {
            let cancelled = self.cancelled.clone();
            let _registration = cancellation.register_interrupt(move || {
                let _ = cancelled.send(());
            });
            self.release
                .lock()
                .expect("release lock should be available")
                .recv()
                .expect("test should release the cancelled mutation");
            return Err(StatusInterfaceError::new(
                StatusInterfaceErrorKind::Command,
                "cancelled",
            ));
        }
        Ok("done".to_owned())
    }
}

impl UiCommandExecutor for BlockingCommands {
    fn execute(
        &self,
        operation: ApplicationOperation,
        cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError> {
        self.started
            .send((thread::current().id(), operation))
            .expect("test should receive worker identity");
        self.release
            .lock()
            .expect("release lock should be available")
            .recv()
            .expect("test should release worker");
        self.finished
            .send(())
            .expect("test should receive completion");
        if cancellation.is_cancelled() {
            Err(StatusInterfaceError::new(
                StatusInterfaceErrorKind::Command,
                "cancelled",
            ))
        } else {
            Ok("done".to_owned())
        }
    }
}

#[derive(Default)]
struct RecordingDispatcher {
    submitted: Vec<Command>,
    results: VecDeque<DispatchedEvent>,
    cancelled: Vec<RequestId>,
    cancelled_all: bool,
    shutdown: bool,
}

struct MutationResyncDispatcher {
    submitted: Vec<Command>,
    results: VecDeque<DispatchedEvent>,
    refreshed: FullViewSnapshot,
}

impl MutationResyncDispatcher {
    fn new(refreshed: FullViewSnapshot) -> Self {
        Self {
            submitted: Vec::new(),
            results: VecDeque::new(),
            refreshed,
        }
    }
}

impl CommandDispatcher for MutationResyncDispatcher {
    fn submit(&mut self, command: Command) -> Result<(), CommandDispatchError> {
        match &command {
            Command::ActivateProfile {
                request_id,
                connection_generation,
                ..
            } => self.results.push_back(DispatchedEvent {
                source: ratash::tui::EventSource::CommandResult,
                event: UiEvent::CommandResult {
                    request_id: *request_id,
                    connection_generation: *connection_generation,
                    result: Ok(MutationSuccess {
                        message: "done".to_owned(),
                    }),
                },
            }),
            Command::RefreshSnapshot {
                connection_generation,
                base_view_revision,
                base_status_revision,
            } => self.results.push_back(DispatchedEvent {
                source: ratash::tui::EventSource::CommandResult,
                event: UiEvent::SnapshotRefreshed {
                    connection_generation: *connection_generation,
                    base_view_revision: *base_view_revision,
                    base_status_revision: *base_status_revision,
                    snapshot: self.refreshed.clone(),
                },
            }),
            _ => {}
        }
        self.submitted.push(command);
        Ok(())
    }

    fn cancel(&mut self, _request_id: RequestId) {}

    fn cancel_all(&mut self) {}

    fn try_next(&mut self) -> Result<Option<DispatchedEvent>, CommandDispatchError> {
        Ok(self.results.pop_front())
    }

    fn shutdown(&mut self) {}
}

impl CommandDispatcher for RecordingDispatcher {
    fn submit(&mut self, command: Command) -> Result<(), CommandDispatchError> {
        self.submitted.push(command);
        Ok(())
    }

    fn cancel(&mut self, request_id: RequestId) {
        self.cancelled.push(request_id);
    }

    fn cancel_all(&mut self) {
        self.cancelled_all = true;
    }

    fn try_next(&mut self) -> Result<Option<DispatchedEvent>, CommandDispatchError> {
        Ok(self.results.pop_front())
    }

    fn shutdown(&mut self) {
        self.shutdown = true;
    }
}

#[derive(Default)]
struct PassiveReconnect;

impl ReconnectTiming for PassiveReconnect {
    fn schedule(&mut self, _connection_generation: u64, _now: Duration) {}

    fn take_due(&mut self, _now: Duration) -> Option<u64> {
        None
    }

    fn reset(&mut self) {}
}

#[derive(Default)]
struct ImmediateReconnect {
    generation: Option<u64>,
}

impl ReconnectTiming for ImmediateReconnect {
    fn schedule(&mut self, connection_generation: u64, _now: Duration) {
        self.generation = Some(connection_generation);
    }

    fn take_due(&mut self, _now: Duration) -> Option<u64> {
        self.generation.take()
    }

    fn reset(&mut self) {
        self.generation = None;
    }
}

struct FixedClock(Duration);

impl RuntimeClock for FixedClock {
    fn now(&self) -> Duration {
        self.0
    }

    fn now_unix_ms(&self) -> u64 {
        0
    }
}

#[derive(Clone)]
struct AdjustableClock {
    state: Arc<Mutex<(Duration, u64)>>,
}

impl AdjustableClock {
    fn new(monotonic: Duration, unix_ms: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new((monotonic, unix_ms))),
        }
    }

    fn advance(&self, duration: Duration) {
        let mut state = self.state.lock().expect("clock state should be available");
        state.0 = state.0.saturating_add(duration);
        state.1 = state
            .1
            .saturating_add(duration.as_millis().try_into().unwrap_or(u64::MAX));
    }
}

impl RuntimeClock for AdjustableClock {
    fn now(&self) -> Duration {
        self.state
            .lock()
            .expect("clock state should be available")
            .0
    }

    fn now_unix_ms(&self) -> u64 {
        self.state
            .lock()
            .expect("clock state should be available")
            .1
    }
}

struct AdvancingWaiter {
    clock: AdjustableClock,
    waits: Mutex<Vec<Option<Duration>>>,
}

impl AdvancingWaiter {
    fn new(clock: AdjustableClock) -> Self {
        Self {
            clock,
            waits: Mutex::new(Vec::new()),
        }
    }

    fn waits(&self) -> Vec<Option<Duration>> {
        self.waits
            .lock()
            .expect("wait recording should be available")
            .clone()
    }
}

impl RuntimeWaiter for AdvancingWaiter {
    fn checkpoint(&self) -> u64 {
        0
    }

    fn wait(&self, _checkpoint: u64, timeout: Option<Duration>) {
        self.waits
            .lock()
            .expect("wait recording should be available")
            .push(timeout);
        if let Some(timeout) = timeout {
            self.clock.advance(timeout);
        }
    }
}

#[derive(Default)]
struct RecordingWaiter {
    waits: Mutex<Vec<Option<Duration>>>,
}

impl RecordingWaiter {
    fn waits(&self) -> Vec<Option<Duration>> {
        self.waits
            .lock()
            .expect("wait recording should be available")
            .clone()
    }
}

impl RuntimeWaiter for RecordingWaiter {
    fn checkpoint(&self) -> u64 {
        0
    }

    fn wait(&self, _checkpoint: u64, timeout: Option<Duration>) {
        self.waits
            .lock()
            .expect("wait recording should be available")
            .push(timeout);
    }
}

struct ImmediateShutdown;

impl ShutdownSignal for ImmediateShutdown {
    fn shutdown_requested(&self) -> bool {
        true
    }
}

struct ScriptedInput {
    polls: usize,
    quit_on: Option<usize>,
}

struct ActivationThenQuitInput {
    polls: usize,
    quit_on: usize,
    profile_id: ProfileId,
}

impl TerminalEventSource for ActivationThenQuitInput {
    fn try_event(&mut self) -> Result<Option<UiEvent>, StatusInterfaceError> {
        self.polls = self.polls.saturating_add(1);
        if self.polls == 1 {
            return Ok(Some(UiEvent::Intent(UiIntent::ActivateProfile(
                self.profile_id,
            ))));
        }
        Ok(
            (self.polls == self.quit_on).then_some(UiEvent::Terminal(TerminalInput::Key(
                KeyInput::Character('q'),
            ))),
        )
    }
}

impl ScriptedInput {
    fn quit_on_poll(poll: usize) -> Self {
        Self {
            polls: 0,
            quit_on: Some(poll),
        }
    }

    fn never() -> Self {
        Self {
            polls: 0,
            quit_on: None,
        }
    }
}

impl TerminalEventSource for ScriptedInput {
    fn try_event(&mut self) -> Result<Option<UiEvent>, StatusInterfaceError> {
        self.polls += 1;
        Ok(
            (self.quit_on == Some(self.polls)).then_some(UiEvent::Terminal(TerminalInput::Key(
                KeyInput::Character('q'),
            ))),
        )
    }
}

struct RecordingRenderer {
    renderer: RatatuiStatusRenderer<TestBackend>,
    uploads: Vec<u64>,
}

impl RecordingRenderer {
    fn new() -> Self {
        Self {
            renderer: RatatuiStatusRenderer::new(TestBackend::new(100, 30))
                .expect("TestBackend should initialize"),
            uploads: Vec::new(),
        }
    }
}

impl StatusRenderer for RecordingRenderer {
    fn draw(
        &mut self,
        state: &ratash::tui::AppState,
    ) -> Result<RenderedFrame, StatusInterfaceError> {
        self.uploads.push(
            state
                .status
                .as_ref()
                .map_or(0, |status| status.traffic.upload_bytes_per_second),
        );
        self.renderer.draw(state)
    }
}

#[derive(Default)]
struct RecordingTerminal {
    actions: Vec<TerminalAction>,
}

impl TerminalControl for RecordingTerminal {
    fn apply(&mut self, action: TerminalAction) -> io::Result<()> {
        self.actions.push(action);
        Ok(())
    }
}
