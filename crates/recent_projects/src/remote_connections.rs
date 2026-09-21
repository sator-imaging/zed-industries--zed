use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};

use anyhow::{Context as _, Result};
use askpass::EncryptedPassword;

use editor::Editor;
use futures::{FutureExt as _, channel::oneshot, future::Shared, lock::Mutex, select_biased};
use gpui::{
    AnyWindowHandle, AppContext, AsyncApp, Entity, Global, PromptLevel, Task, WindowHandle,
};

use project::trusted_worktrees;
use remote::{
    DockerConnectionOptions, Interactive, RemoteConnection, RemoteConnectionIdentity,
    RemoteConnectionOptions, SshConnectionOptions,
};
pub use settings::SshConnection;
use settings::{DevContainerConnection, ExtendingVec, RegisterSetting, Settings, WslConnection};
use util::{path_list::PathList, paths::PathWithPosition};
use workspace::{
    AppState, MultiWorkspace, OpenOptions, SerializedWorkspaceLocation, Workspace,
    find_existing_workspace,
};

pub use remote_connection::{
    RemoteClientDelegate, RemoteConnectionModal, RemoteConnectionPrompt, SshConnectionHeader,
    connect,
};

#[derive(RegisterSetting)]
pub struct RemoteSettings {
    pub ssh_connections: ExtendingVec<SshConnection>,
    pub wsl_connections: ExtendingVec<WslConnection>,
    /// Whether to read ~/.ssh/config for ssh connection sources.
    pub read_ssh_config: bool,
}

impl RemoteSettings {
    pub fn ssh_connections(&self) -> impl Iterator<Item = SshConnection> + use<> {
        self.ssh_connections.clone().0.into_iter()
    }

    pub fn wsl_connections(&self) -> impl Iterator<Item = WslConnection> + use<> {
        self.wsl_connections.clone().0.into_iter()
    }

    pub fn fill_connection_options_from_settings(&self, options: &mut SshConnectionOptions) {
        for conn in self.ssh_connections() {
            if conn.host == options.host.to_string()
                && conn.username == options.username
                && conn.port == options.port
            {
                options.nickname = conn.nickname;
                options.upload_binary_over_ssh = conn.upload_binary_over_ssh.unwrap_or_default();
                options.args = Some(conn.args);
                options.port_forwards = conn.port_forwards;
                break;
            }
        }
    }

    pub fn connection_options_for(
        &self,
        host: String,
        port: Option<u16>,
        username: Option<String>,
    ) -> SshConnectionOptions {
        let mut options = SshConnectionOptions {
            host: host.into(),
            port,
            username,
            ..Default::default()
        };
        self.fill_connection_options_from_settings(&mut options);
        options
    }
}

#[derive(Clone, PartialEq)]
pub enum Connection {
    Ssh(SshConnection),
    Wsl(WslConnection),
    DevContainer(DevContainerConnection),
}

impl From<Connection> for RemoteConnectionOptions {
    fn from(val: Connection) -> Self {
        match val {
            Connection::Ssh(conn) => RemoteConnectionOptions::Ssh(conn.into()),
            Connection::Wsl(conn) => RemoteConnectionOptions::Wsl(conn.into()),
            Connection::DevContainer(conn) => {
                RemoteConnectionOptions::Docker(DockerConnectionOptions {
                    name: conn.name,
                    remote_user: conn.remote_user,
                    container_id: conn.container_id,
                    upload_binary_over_docker_exec: false,
                    use_podman: conn.use_podman,
                    remote_env: conn.remote_env,
                })
            }
        }
    }
}

impl From<SshConnection> for Connection {
    fn from(val: SshConnection) -> Self {
        Connection::Ssh(val)
    }
}

impl From<WslConnection> for Connection {
    fn from(val: WslConnection) -> Self {
        Connection::Wsl(val)
    }
}

impl Settings for RemoteSettings {
    fn from_settings(content: &settings::SettingsContent) -> Self {
        let remote = &content.remote;
        Self {
            ssh_connections: remote.ssh_connections.clone().unwrap_or_default().into(),
            wsl_connections: remote.wsl_connections.clone().unwrap_or_default().into(),
            read_ssh_config: remote.read_ssh_config.unwrap(),
        }
    }
}

pub struct PreparedRemoteProject {
    connection_options: RemoteConnectionOptions,
    paths: Vec<PathBuf>,
    app_state: Arc<AppState>,
    open_options: OpenOptions,
    window: WindowHandle<MultiWorkspace>,
    initial_workspace: Entity<Workspace>,
    attempt: RemoteConnectionAttempt,
}

pub async fn prepare_remote_project(
    connection_options: RemoteConnectionOptions,
    paths: Vec<PathBuf>,
    app_state: Arc<AppState>,
    cx: &mut AsyncApp,
) -> Result<PreparedRemoteProject> {
    prepare_remote_project_inner(
        connection_options,
        paths,
        app_state,
        OpenOptions::default(),
        cx,
    )
    .await
}

pub async fn open_remote_project(
    connection_options: RemoteConnectionOptions,
    paths: Vec<PathBuf>,
    app_state: Arc<AppState>,
    open_options: OpenOptions,
    cx: &mut AsyncApp,
) -> Result<WindowHandle<MultiWorkspace>> {
    if let Some(window) =
        open_existing_remote_project(&connection_options, &paths, None, &open_options, true, cx)
            .await?
    {
        return Ok(window);
    }

    prepare_remote_project_inner(connection_options, paths, app_state, open_options, cx)
        .await?
        .connect(true, cx)
        .await
}

impl PreparedRemoteProject {
    pub fn window(&self) -> WindowHandle<MultiWorkspace> {
        self.window
    }

    pub async fn connect(
        self,
        activate_window: bool,
        cx: &mut AsyncApp,
    ) -> Result<WindowHandle<MultiWorkspace>> {
        let Self {
            connection_options,
            paths,
            app_state,
            open_options,
            window,
            initial_workspace,
            mut attempt,
        } = self;
        let created_new_window = open_options.requesting_window.is_none();

        loop {
            let RemoteConnectionAttempt {
                delegate,
                modal,
                cancelled,
                cancel_rx,
                cancellation_task,
            } = attempt;
            let mut project_open_failed = false;
            let result = {
                let opening = async {
                    window.update(cx, |_, window, _| {
                        if activate_window {
                            window.activate_window();
                        }
                    })?;
                    if let Some(existing) = open_existing_remote_project(
                        &connection_options,
                        &paths,
                        None,
                        &open_options,
                        activate_window,
                        cx,
                    )
                    .await?
                    {
                        return Ok(Some(existing));
                    }
                    let remote_connection =
                        remote::connect(connection_options.clone(), delegate.clone(), cx).await?;
                    let (resolved_paths, paths_with_positions) =
                        determine_paths_with_positions(&remote_connection, paths.clone()).await;
                    let open_lock = cx.update(|cx| {
                        cx.default_global::<RemoteProjectOpenLocks>()
                            .lock(&connection_options, &resolved_paths)
                    });
                    let _guard = open_lock.lock().await;
                    if let Some(existing) = open_existing_remote_project(
                        &connection_options,
                        &resolved_paths,
                        Some(&paths_with_positions),
                        &open_options,
                        activate_window,
                        cx,
                    )
                    .await?
                    {
                        return Ok(Some(existing));
                    }

                    let (workspace, items) = cx
                        .update(|cx| {
                            workspace::open_remote_project_with_new_connection(
                                window,
                                remote_connection,
                                cancel_rx,
                                delegate,
                                app_state.clone(),
                                resolved_paths,
                                activate_window,
                                cx,
                            )
                        })
                        .await
                        .inspect_err(|_| project_open_failed = true)?;
                    if workspace.is_none() {
                        return Ok(None);
                    }
                    navigate_to_positions(&window, items, &paths_with_positions, cx);
                    anyhow::Ok(Some(window))
                };
                select_biased! {
                    _ = cancelled.fuse() => Ok(None),
                    _ = cancellation_task.fuse() => Ok(None),
                    result = opening.fuse() => result,
                }
            };
            modal.update(cx, |modal, cx| modal.finished(cx));

            match result {
                Ok(Some(opened_window)) => {
                    if created_new_window && opened_window != window {
                        window
                            .update(cx, |_, window, _| window.remove_window())
                            .ok();
                    }
                    return Ok(opened_window);
                }
                Ok(None) => break,
                Err(error) => {
                    let Ok(response) = window.update(cx, |_, window, cx| {
                        log::error!("Failed to open project: {error:#}");
                        window.prompt(
                            PromptLevel::Critical,
                            match connection_options {
                                RemoteConnectionOptions::Ssh(_) => "Failed to connect over SSH",
                                RemoteConnectionOptions::Wsl(_) => "Failed to connect to WSL",
                                RemoteConnectionOptions::Docker(_) => {
                                    "Failed to connect to Dev Container"
                                }
                                #[cfg(any(test, feature = "test-support"))]
                                RemoteConnectionOptions::Mock(_) => {
                                    "Failed to connect to mock server"
                                }
                            },
                            Some(&format!("{error:#}")),
                            &["Retry", "Cancel"],
                            cx,
                        )
                    }) else {
                        return Ok(window);
                    };
                    if response.await != Ok(0) {
                        if project_open_failed {
                            initial_workspace.update(cx, |workspace, cx| {
                                trusted_worktrees::track_worktree_trust(
                                    workspace.project().read(cx).worktree_store(),
                                    None,
                                    None,
                                    None,
                                    cx,
                                );
                            });
                        }
                        break;
                    }
                    let Some(next_attempt) = prepare_remote_connection_attempt(
                        window,
                        &initial_workspace,
                        &connection_options,
                        &paths,
                        created_new_window,
                        cx,
                    ) else {
                        break;
                    };
                    attempt = next_attempt;
                }
            }
        }

        if created_new_window {
            window
                .update(cx, |_, window, _| window.remove_window())
                .ok();
        }
        Ok(window)
    }
}

pub fn navigate_to_positions(
    window: &WindowHandle<MultiWorkspace>,
    items: impl IntoIterator<Item = Option<Box<dyn workspace::item::ItemHandle>>>,
    positions: &[PathWithPosition],
    cx: &mut AsyncApp,
) {
    for (item, path) in items.into_iter().zip(positions) {
        let Some(item) = item else {
            continue;
        };
        let Some(row) = path.row else {
            continue;
        };
        if let Some(active_editor) = item.downcast::<Editor>() {
            window
                .update(cx, |_, window, cx| {
                    active_editor.update(cx, |editor, cx| {
                        let row = row.saturating_sub(1);
                        let col = path.column.unwrap_or(0).saturating_sub(1);
                        let Some(buffer) = editor.buffer().read(cx).as_singleton() else {
                            return;
                        };
                        let buffer_snapshot = buffer.read(cx).snapshot();
                        let point = buffer_snapshot.point_from_external_input(row, col);
                        editor.go_to_singleton_buffer_point(point, window, cx);
                    });
                })
                .ok();
        }
    }
}

pub(crate) async fn determine_paths_with_positions(
    remote_connection: &Arc<dyn RemoteConnection>,
    mut paths: Vec<PathBuf>,
) -> (Vec<PathBuf>, Vec<PathWithPosition>) {
    let mut paths_with_positions = Vec::<PathWithPosition>::new();
    for path in &mut paths {
        if let Some(path_str) = path.to_str() {
            let path_with_position = PathWithPosition::parse_str(&path_str);
            if path_with_position.row.is_some() {
                if !path_exists(&remote_connection, &path).await {
                    *path = path_with_position.path.clone();
                    paths_with_positions.push(path_with_position);
                    continue;
                }
            }
        }
        paths_with_positions.push(PathWithPosition::from_path(path.clone()))
    }
    (paths, paths_with_positions)
}

struct RemoteConnectionAttempt {
    delegate: Arc<RemoteClientDelegate>,
    modal: Entity<RemoteConnectionModal>,
    cancelled: Shared<oneshot::Receiver<()>>,
    cancel_rx: oneshot::Receiver<()>,
    cancellation_task: Task<()>,
}

#[derive(Default)]
struct RemoteProjectOpenLocks {
    locks: HashMap<(RemoteConnectionIdentity, PathList), Weak<Mutex<()>>>,
}

impl Global for RemoteProjectOpenLocks {}

impl RemoteProjectOpenLocks {
    fn lock(&mut self, options: &RemoteConnectionOptions, paths: &[PathBuf]) -> Arc<Mutex<()>> {
        self.locks.retain(|_, lock| lock.strong_count() > 0);
        let entry = self
            .locks
            .entry((
                RemoteConnectionIdentity::from(options),
                PathList::new(paths),
            ))
            .or_default();
        if let Some(lock) = entry.upgrade() {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        *entry = Arc::downgrade(&lock);
        lock
    }
}

async fn open_existing_remote_project(
    connection_options: &RemoteConnectionOptions,
    paths: &[PathBuf],
    positions: Option<&[PathWithPosition]>,
    open_options: &OpenOptions,
    activate_window: bool,
    cx: &mut AsyncApp,
) -> Result<Option<WindowHandle<MultiWorkspace>>> {
    let (existing, open_visible) = find_existing_workspace(
        paths,
        open_options,
        &SerializedWorkspaceLocation::Remote(connection_options.clone()),
        cx,
    )
    .await;
    let Some((window, workspace)) = existing else {
        return Ok(None);
    };
    let remote_connection = cx.update(|cx| {
        workspace
            .read(cx)
            .project()
            .read(cx)
            .remote_client()
            .and_then(|client| client.read(cx).remote_connection())
    });
    let Some(remote_connection) = remote_connection else {
        // If the remote connection is dead (e.g. server not running after failed reconnect),
        // fall through to establish a fresh connection instead of showing an error.
        log::info!(
            "existing remote workspace found but connection is dead, starting fresh connection"
        );
        return Ok(None);
    };
    let (resolved_paths, paths_with_positions) = if let Some(positions) = positions {
        (paths.to_vec(), positions.to_vec())
    } else {
        determine_paths_with_positions(&remote_connection, paths.to_vec()).await
    };
    let open_results = window
        .update(cx, |multi_workspace, window, cx| {
            if activate_window {
                window.activate_window();
            }
            multi_workspace.activate(workspace.clone(), None, window, cx);
            workspace.update(cx, |workspace, cx| {
                workspace.open_paths(
                    resolved_paths,
                    OpenOptions {
                        visible: Some(open_visible),
                        ..OpenOptions::default()
                    },
                    None,
                    window,
                    cx,
                )
            })
        })?
        .await;
    workspace.update(cx, |workspace, cx| {
        for result in open_results.iter().flatten() {
            if let Err(error) = result {
                workspace.show_error(format!("{error}"), cx);
            }
        }
    });
    let items = open_results
        .into_iter()
        .map(|result| result.and_then(Result::ok));
    navigate_to_positions(&window, items, &paths_with_positions, cx);
    Ok(Some(window))
}

async fn prepare_remote_project_inner(
    connection_options: RemoteConnectionOptions,
    paths: Vec<PathBuf>,
    app_state: Arc<AppState>,
    open_options: OpenOptions,
    cx: &mut AsyncApp,
) -> Result<PreparedRemoteProject> {
    let created_new_window = open_options.requesting_window.is_none();
    let (window, initial_workspace) = if let Some(window) = open_options.requesting_window {
        let workspace = window.update(cx, |multi_workspace, _, _| {
            multi_workspace.workspace().clone()
        })?;
        (window, workspace)
    } else {
        let workspace_position = cx
            .update(|cx| {
                workspace::remote_workspace_position_from_db(connection_options.clone(), &paths, cx)
            })
            .await
            .context("fetching remote workspace position from db")?;

        let mut options =
            cx.update(|cx| (app_state.build_window_options)(workspace_position.display, cx));
        options.window_bounds = workspace_position.window_bounds;

        let window = cx.open_window(options, |window, cx| {
            let project = project::Project::local(
                app_state.client.clone(),
                app_state.node_runtime.clone(),
                app_state.user_store.clone(),
                app_state.languages.clone(),
                app_state.fs.clone(),
                None,
                project::LocalProjectFlags {
                    init_worktree_trust: false,
                    ..Default::default()
                },
                cx,
            );
            let workspace = cx.new(|cx| {
                let mut workspace = Workspace::new(None, project, app_state.clone(), window, cx);
                workspace.mark_as_remote_connection_placeholder();
                workspace.centered_layout = workspace_position.centered_layout;
                workspace
            });
            cx.new(|cx| MultiWorkspace::new(workspace, window, cx))
        })?;
        let workspace = window.update(cx, |multi_workspace, window, _cx| {
            window.activate_window();
            multi_workspace.workspace().clone()
        })?;
        (window, workspace)
    };

    let attempt = prepare_remote_connection_attempt(
        window,
        &initial_workspace,
        &connection_options,
        &paths,
        created_new_window,
        cx,
    )
    .context("remote connection modal could not be opened")?;

    Ok(PreparedRemoteProject {
        connection_options,
        paths,
        app_state,
        open_options,
        window,
        initial_workspace,
        attempt,
    })
}

fn prepare_remote_connection_attempt(
    window: WindowHandle<MultiWorkspace>,
    initial_workspace: &Entity<Workspace>,
    connection_options: &RemoteConnectionOptions,
    paths: &[PathBuf],
    created_new_window: bool,
    cx: &mut AsyncApp,
) -> Option<RemoteConnectionAttempt> {
    let (cancel_tx, modal_cancel_rx) = oneshot::channel();
    let modal = window
        .update(cx, |_, window, cx| {
            initial_workspace.update(cx, |workspace, cx| {
                workspace.hide_modal(window, cx);
                workspace.toggle_modal(window, cx, |window, cx| {
                    RemoteConnectionModal::new(connection_options, paths.to_vec(), window, cx)
                });
                let modal = workspace.active_modal::<RemoteConnectionModal>(cx)?;
                let prompt = modal.read(cx).prompt.clone();
                prompt.update(cx, |prompt, _| prompt.set_cancellation_tx(cancel_tx));
                Some(modal)
            })
        })
        .ok()??;
    let delegate = Arc::new(RemoteClientDelegate::new(
        AnyWindowHandle::from(window),
        modal.read_with(cx, |modal, _| modal.prompt.downgrade()),
        if let RemoteConnectionOptions::Ssh(options) = connection_options {
            options
                .password
                .as_deref()
                .and_then(|password| EncryptedPassword::try_from(password).ok())
        } else {
            None
        },
    ));
    let (closed_tx, closed_rx) = oneshot::channel();
    let mut closed_tx = Some(closed_tx);
    let subscription = cx.update(|cx| {
        cx.on_window_closed(move |_, closed_window| {
            if closed_window == window.window_id()
                && let Some(sender) = closed_tx.take()
            {
                sender.send(()).ok();
            }
        })
    });
    let (connection_cancel_tx, cancel_rx) = oneshot::channel();
    let cancelled = modal_cancel_rx.shared();
    let modal_cancelled = cancelled.clone();
    let cancellation_task = cx.spawn(async move |cx| {
        let _subscription = subscription;
        select_biased! {
            _ = modal_cancelled.fuse() => {},
            _ = closed_rx.fuse() => {},
        }
        connection_cancel_tx.send(()).ok();
        if created_new_window {
            window
                .update(cx, |_, window, _| window.remove_window())
                .ok();
        }
    });
    Some(RemoteConnectionAttempt {
        delegate,
        modal,
        cancelled,
        cancel_rx,
        cancellation_task,
    })
}

async fn path_exists(connection: &Arc<dyn RemoteConnection>, path: &Path) -> bool {
    let Ok(command) = connection.build_command(
        Some("test".to_string()),
        &["-e".to_owned(), path.to_string_lossy().to_string()],
        &Default::default(),
        None,
        None,
        Interactive::No,
    ) else {
        return false;
    };
    let Ok(mut child) = util::command::new_command(command.program)
        .args(command.args)
        .envs(command.env)
        .spawn()
    else {
        return false;
    };
    child.status().await.is_ok_and(|status| status.success())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disconnected_overlay::DisconnectedOverlay;
    use extension::ExtensionHostProxy;
    use fs::FakeFs;
    use gpui::{AppContext, TestAppContext};
    use http_client::BlockedHttpClient;
    use language::Point;
    use node_runtime::NodeRuntime;
    use remote::{ConnectionState, MockConnectionOptions, RemoteClient};
    use remote_server::{HeadlessAppState, HeadlessProject};
    use serde_json::json;
    use util::path;
    use workspace::find_existing_workspace;

    #[gpui::test]
    async fn test_open_remote_project_with_mock_connection(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        remote_fs
            .insert_tree(
                path!("/project"),
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                    },
                    "README.md": "# Test Project",
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let http_client = Arc::new(BlockedHttpClient);
        let node_runtime = NodeRuntime::unavailable();
        let languages = Arc::new(language::LanguageRegistry::new(server_cx.executor()));
        let proxy = Arc::new(ExtensionHostProxy::new());

        let _headless = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session: server_session,
                    fs: remote_fs.clone(),
                    http_client,
                    node_runtime,
                    languages,
                    extension_host_proxy: proxy,
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });

        drop(connect_guard);

        let paths = vec![PathBuf::from(path!("/project"))];
        let open_options = workspace::OpenOptions::default();

        let mut async_cx = cx.to_async();
        let result = open_remote_project(opts, paths, app_state, open_options, &mut async_cx).await;

        executor.run_until_parked();

        assert!(result.is_ok(), "open_remote_project should succeed");

        let windows = cx.update(|cx| cx.windows().len());
        assert_eq!(windows, 1, "Should have opened a window");

        let multi_workspace_handle =
            cx.update(|cx| cx.windows()[0].downcast::<MultiWorkspace>().unwrap());

        multi_workspace_handle
            .update(cx, |multi_workspace, _, cx| {
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    let project = workspace.project().read(cx);
                    assert!(project.is_remote(), "Project should be a remote project");
                });
            })
            .unwrap();
    }

    #[gpui::test]
    async fn test_reuse_existing_remote_workspace_window(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        remote_fs
            .insert_tree(
                path!("/project"),
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                        "lib.rs": "pub fn hello() {}",
                    },
                    "README.md": "# Test Project",
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let http_client = Arc::new(BlockedHttpClient);
        let node_runtime = NodeRuntime::unavailable();
        let languages = Arc::new(language::LanguageRegistry::new(server_cx.executor()));
        let proxy = Arc::new(ExtensionHostProxy::new());

        let _headless = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session: server_session,
                    fs: remote_fs.clone(),
                    http_client,
                    node_runtime,
                    languages,
                    extension_host_proxy: proxy,
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });

        drop(connect_guard);

        // First open: create a new window for the remote project.
        let paths = vec![PathBuf::from(path!("/project"))];
        let mut async_cx = cx.to_async();
        open_remote_project(
            opts.clone(),
            paths,
            app_state.clone(),
            workspace::OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .expect("first open_remote_project should succeed");

        executor.run_until_parked();

        assert_eq!(
            cx.update(|cx| cx.windows().len()),
            1,
            "First open should create exactly one window"
        );

        let first_window = cx.update(|cx| cx.windows()[0].downcast::<MultiWorkspace>().unwrap());

        // Verify find_existing_workspace discovers the remote workspace.
        let search_paths = vec![PathBuf::from(path!("/project/src/lib.rs"))];
        let (found, _open_visible) = find_existing_workspace(
            &search_paths,
            &workspace::OpenOptions::default(),
            &SerializedWorkspaceLocation::Remote(opts.clone()),
            &mut async_cx,
        )
        .await;

        assert!(
            found.is_some(),
            "find_existing_workspace should locate the existing remote workspace"
        );
        let (found_window, _found_workspace) = found.unwrap();
        assert_eq!(
            found_window, first_window,
            "find_existing_workspace should return the same window"
        );

        // Second open with the same connection options should reuse the window.
        let second_paths = vec![PathBuf::from(path!("/project/src/lib.rs"))];
        open_remote_project(
            opts.clone(),
            second_paths,
            app_state.clone(),
            workspace::OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .expect("second open_remote_project should succeed via reuse");

        executor.run_until_parked();

        assert_eq!(
            cx.update(|cx| cx.windows().len()),
            1,
            "Second open should reuse the existing window, not create a new one"
        );

        let still_first_window =
            cx.update(|cx| cx.windows()[0].downcast::<MultiWorkspace>().unwrap());
        assert_eq!(
            still_first_window, first_window,
            "The window handle should be the same after reuse"
        );
    }

    #[gpui::test]
    async fn test_reopen_existing_remote_root_treats_root_as_directory(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        let remote_home = paths::home_dir();
        let canonical_project_path = remote_home.join("remote-reopen-root-project");
        remote_fs
            .insert_tree(
                &canonical_project_path,
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                    },
                    "README.md": "# Test Project",
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let http_client = Arc::new(BlockedHttpClient);
        let node_runtime = NodeRuntime::unavailable();
        let languages = Arc::new(language::LanguageRegistry::new(server_cx.executor()));
        let proxy = Arc::new(ExtensionHostProxy::new());

        let _headless = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session: server_session,
                    fs: remote_fs.clone(),
                    http_client,
                    node_runtime,
                    languages,
                    extension_host_proxy: proxy,
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });

        drop(connect_guard);

        let mut async_cx = cx.to_async();
        let window = open_remote_project(
            opts,
            vec![canonical_project_path.clone()],
            app_state,
            workspace::OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .expect("initial open_remote_project should succeed");

        executor.run_until_parked();

        let open_results = window
            .update(cx, |multi_workspace, window, cx| {
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    workspace.open_paths(
                        vec![canonical_project_path.clone()],
                        workspace::OpenOptions {
                            visible: Some(workspace::OpenVisible::All),
                            ..Default::default()
                        },
                        None,
                        window,
                        cx,
                    )
                })
            })
            .unwrap()
            .await;

        assert_eq!(open_results.len(), 1, "should return one open result");
        assert!(
            open_results[0].is_none(),
            "reopening a remote root directory should not try to open it as a file"
        );
    }

    #[gpui::test]
    async fn test_reconnect_when_server_not_running(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        remote_fs
            .insert_tree(
                path!("/project"),
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                    },
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let http_client = Arc::new(BlockedHttpClient);
        let node_runtime = NodeRuntime::unavailable();
        let languages = Arc::new(language::LanguageRegistry::new(server_cx.executor()));
        let proxy = Arc::new(ExtensionHostProxy::new());

        let _headless = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session: server_session,
                    fs: remote_fs.clone(),
                    http_client: http_client.clone(),
                    node_runtime: node_runtime.clone(),
                    languages: languages.clone(),
                    extension_host_proxy: proxy.clone(),
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });

        drop(connect_guard);

        // Open the remote project normally.
        let paths = vec![PathBuf::from(path!("/project"))];
        let mut async_cx = cx.to_async();
        open_remote_project(
            opts.clone(),
            paths.clone(),
            app_state.clone(),
            workspace::OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .expect("initial open should succeed");

        executor.run_until_parked();

        assert_eq!(cx.update(|cx| cx.windows().len()), 1);
        let window = cx.update(|cx| cx.windows()[0].downcast::<MultiWorkspace>().unwrap());

        // Force the remote client into ServerNotRunning state (simulates the
        // scenario where the remote server died and reconnection failed).
        let original_client = window
            .read_with(cx, |multi_workspace, cx| {
                multi_workspace
                    .workspace()
                    .read(cx)
                    .project()
                    .read(cx)
                    .remote_client()
                    .expect("should have remote client")
                    .clone()
            })
            .unwrap();
        original_client.update(cx, |client, cx| client.force_server_not_running(cx));

        executor.run_until_parked();
        window
            .read_with(cx, |multi_workspace, cx| {
                assert!(
                    multi_workspace
                        .workspace()
                        .read(cx)
                        .active_modal::<DisconnectedOverlay>(cx)
                        .is_some()
                );
            })
            .unwrap();
        cx.dispatch_action(AnyWindowHandle::from(window), menu::Cancel);
        window
            .read_with(cx, |multi_workspace, cx| {
                assert!(
                    multi_workspace
                        .workspace()
                        .read(cx)
                        .active_modal::<DisconnectedOverlay>(cx)
                        .is_none()
                );
            })
            .unwrap();

        // Register a new mock server under the same options so the reconnect
        // path can establish a fresh connection.
        let (server_session_2, connect_guard_2) =
            RemoteClient::fake_server_with_opts(&opts, cx, server_cx);

        let _headless_2 = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session: server_session_2,
                    fs: remote_fs.clone(),
                    http_client,
                    node_runtime,
                    languages,
                    extension_host_proxy: proxy,
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });

        drop(connect_guard_2);

        let result = open_remote_project(
            opts,
            paths,
            app_state,
            workspace::OpenOptions {
                requesting_window: Some(window),
                ..Default::default()
            },
            &mut async_cx,
        )
        .await;

        executor.run_until_parked();

        assert!(
            result.is_ok(),
            "reconnect should succeed but got: {:?}",
            result.err()
        );

        // Should still be a single window with a working remote project.
        assert_eq!(cx.update(|cx| cx.windows().len()), 1);

        window
            .update(cx, |multi_workspace, _, cx| {
                let workspace = multi_workspace.workspace().clone();
                let client = workspace
                    .read(cx)
                    .project()
                    .read(cx)
                    .remote_client()
                    .expect("should have remote client after reconnect")
                    .clone();
                assert_ne!(client, original_client);
                assert_eq!(
                    client.read(cx).connection_state(),
                    ConnectionState::Connected
                );
            })
            .unwrap();
        assert!(!cx.has_pending_prompt());
    }

    #[gpui::test]
    async fn test_prepared_remote_project_reuses_completed_open(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let (options, _server, connect_guard) = init_remote_server(None, cx, server_cx).await;
        let paths = vec![PathBuf::from(path!("/project"))];
        let prepared = prepare_remote_project(
            options.clone(),
            paths.clone(),
            app_state.clone(),
            &mut cx.to_async(),
        )
        .await
        .unwrap();
        let placeholder = prepared.window;
        drop(connect_guard);
        let window = open_remote_project(
            options,
            paths,
            app_state,
            OpenOptions::default(),
            &mut cx.to_async(),
        )
        .await
        .unwrap();
        workspace::flush_windows_serialization(&[window], &mut cx.to_async()).await;
        let workspace = window
            .read_with(cx, |window, _| window.workspace().clone())
            .unwrap();

        let restored = prepared.connect(false, &mut cx.to_async()).await.unwrap();
        cx.run_until_parked();

        assert_eq!(restored, window);
        assert_eq!(cx.windows(), vec![AnyWindowHandle::from(window)]);
        assert!(placeholder.read_with(cx, |_, _| ()).is_err());
        assert_eq!(
            window
                .read_with(cx, |window, _| window.workspace().clone())
                .unwrap(),
            workspace,
        );
        assert!(!cx.has_pending_prompt());
    }

    #[gpui::test]
    async fn test_prepared_remote_project_reuses_pending_open(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        for (open_path, restore_path) in [
            (path!("/project"), path!("/project")),
            (path!("/project/file.txt"), path!("/project/file.txt:1:4")),
        ] {
            let (options, _server, connect_guard) = init_remote_server(None, cx, server_cx).await;
            let prepared = prepare_remote_project(
                options.clone(),
                vec![PathBuf::from(restore_path)],
                app_state.clone(),
                &mut cx.to_async(),
            )
            .await
            .unwrap();
            let opening = cx.spawn({
                let app_state = app_state.clone();
                async move |mut cx| {
                    open_remote_project(
                        options,
                        vec![PathBuf::from(open_path)],
                        app_state,
                        OpenOptions::default(),
                        &mut cx,
                    )
                    .await
                    .unwrap()
                }
            });
            cx.run_until_parked();
            assert!(!opening.is_ready());
            let restoring =
                cx.spawn(async move |mut cx| prepared.connect(false, &mut cx).await.unwrap());
            cx.run_until_parked();
            assert!(!restoring.is_ready());
            assert_eq!(cx.windows().len(), 2);

            drop(connect_guard);
            let window = opening.await;
            let restored = restoring.await;
            cx.run_until_parked();

            assert_eq!(restored, window);
            assert_eq!(cx.windows(), vec![AnyWindowHandle::from(window)]);
            assert!(!cx.has_pending_prompt());
            if open_path != restore_path {
                window
                    .update(cx, |multi_workspace, _, cx| {
                        let editor = multi_workspace
                            .workspace()
                            .read(cx)
                            .active_item_as::<Editor>(cx)
                            .unwrap();
                        editor.update(cx, |editor, cx| {
                            let snapshot = editor.display_snapshot(cx);
                            assert_eq!(
                                editor.selections.newest::<Point>(&snapshot).range(),
                                Point::new(0, 3)..Point::new(0, 3),
                            );
                        });
                    })
                    .unwrap();
            }
            window
                .update(cx, |_, window, _| window.remove_window())
                .unwrap();
            cx.run_until_parked();
        }
    }

    #[gpui::test]
    async fn test_closing_prepared_remote_project_cancels_wait_for_pending_open(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let (options, _server, connect_guard) = init_remote_server(None, cx, server_cx).await;
        let paths = vec![PathBuf::from(path!("/project"))];
        let prepared = prepare_remote_project(
            options.clone(),
            paths.clone(),
            app_state.clone(),
            &mut cx.to_async(),
        )
        .await
        .unwrap();
        let placeholder = prepared.window;
        let opening = cx.spawn(async move |mut cx| {
            open_remote_project(options, paths, app_state, OpenOptions::default(), &mut cx)
                .await
                .unwrap()
        });
        cx.run_until_parked();
        let restoring =
            cx.spawn(async move |mut cx| prepared.connect(false, &mut cx).await.unwrap());
        cx.run_until_parked();
        assert!(!restoring.is_ready());

        placeholder
            .update(cx, |_, window, _| window.remove_window())
            .unwrap();
        cx.run_until_parked();
        assert!(restoring.is_ready());
        assert_eq!(restoring.await, placeholder);
        assert!(!opening.is_ready());
        assert_eq!(cx.windows().len(), 1);

        drop(connect_guard);
        let window = opening.await;
        cx.run_until_parked();
        assert_eq!(cx.windows(), vec![AnyWindowHandle::from(window)]);
        assert!(!cx.has_pending_prompt());
    }

    #[gpui::test]
    async fn test_prepared_remote_project_cancels_before_and_during_connection(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        for start_connection in [false, true] {
            let (options, _server, _connect_guard) = init_remote_server(None, cx, server_cx).await;
            let prepared = prepare_remote_project(
                options,
                vec![PathBuf::from(path!("/project"))],
                app_state.clone(),
                &mut cx.to_async(),
            )
            .await
            .unwrap();
            let window = prepared.window;
            let (start_tx, start_rx) = oneshot::channel();
            let connecting = cx.spawn(async move |mut cx| {
                start_rx.await.unwrap();
                prepared.connect(false, &mut cx).await.unwrap()
            });
            let start_tx = if start_connection {
                start_tx.send(()).unwrap();
                None
            } else {
                Some(start_tx)
            };
            cx.run_until_parked();
            assert!(!connecting.is_ready());

            cx.dispatch_action(AnyWindowHandle::from(window), menu::Cancel);
            cx.run_until_parked();
            assert_eq!(cx.windows().len(), 0);
            if let Some(start_tx) = start_tx {
                start_tx.send(()).unwrap();
            }
            assert_eq!(connecting.await, window);
            assert!(!cx.has_pending_prompt());
        }
    }

    #[gpui::test]
    async fn test_prepared_remote_project_retries_connection_failure(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let options = RemoteConnectionOptions::Mock(MockConnectionOptions { id: u64::MAX });
        let prepared = prepare_remote_project(
            options.clone(),
            vec![PathBuf::from(path!("/project"))],
            app_state,
            &mut cx.to_async(),
        )
        .await
        .unwrap();
        let window = prepared.window;
        let connecting =
            cx.spawn(async move |mut cx| prepared.connect(false, &mut cx).await.unwrap());
        cx.run_until_parked();
        assert!(cx.has_pending_prompt());
        assert!(!connecting.is_ready());

        let (_, _server, connect_guard) = init_remote_server(Some(&options), cx, server_cx).await;
        drop(connect_guard);
        cx.simulate_prompt_answer("Retry");
        assert_eq!(connecting.await, window);
        cx.run_until_parked();
        assert_eq!(cx.windows(), vec![AnyWindowHandle::from(window)]);
        assert!(!cx.has_pending_prompt());
        window
            .read_with(cx, |window, cx| {
                assert!(window.workspace().read(cx).project().read(cx).is_remote());
            })
            .unwrap();
    }

    async fn init_remote_server(
        options: Option<&RemoteConnectionOptions>,
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) -> (
        RemoteConnectionOptions,
        Entity<HeadlessProject>,
        oneshot::Sender<()>,
    ) {
        cx.update(|cx| release_channel::init(semver::Version::new(0, 0, 0), cx));
        server_cx.update(|cx| release_channel::init(semver::Version::new(0, 0, 0), cx));
        let (options, session, connect_guard) = if let Some(options) = options {
            let (session, connect_guard) =
                RemoteClient::fake_server_with_opts(options, cx, server_cx);
            (options.clone(), session, connect_guard)
        } else {
            RemoteClient::fake_server(cx, server_cx)
        };
        let fs = FakeFs::new(server_cx.executor());
        fs.insert_tree(path!("/project"), json!({"file.txt": "remote file"}))
            .await;
        server_cx.update(HeadlessProject::init);
        let languages = Arc::new(language::LanguageRegistry::new(server_cx.executor()));
        let server = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session,
                    fs,
                    http_client: Arc::new(BlockedHttpClient),
                    node_runtime: NodeRuntime::unavailable(),
                    languages,
                    extension_host_proxy: Arc::new(ExtensionHostProxy::new()),
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });
        (options, server, connect_guard)
    }

    fn init_test(cx: &mut TestAppContext) -> Arc<AppState> {
        cx.update(|cx| {
            let state = AppState::test(cx);
            crate::init(cx);
            editor::init(cx);
            state
        })
    }
}
