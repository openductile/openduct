// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState, type FormEvent } from "react";
import { api, setWorkspace, type Session } from "./api";
import { Credentials } from "./Credentials";
import { Files } from "./Files";
import { Members } from "./Members";
import { Sidebar, useFolded, useNarrow } from "./Sidebar";
import { Editor } from "./Editor";
import { Settings } from "./Settings";
import { Wordmark } from "./Wordmark";
import { Logs, Runs } from "./Watch";
import { AllWorkspaces, Dashboard } from "./Dashboard";
import { Pipelines } from "./Pipelines";
import { Bell } from "./Bell";
import { TabBar, useTabs } from "./Tabs";
import { Templates, Workflows } from "./Workflows";

type View = { kind: "loading" } | { kind: "setup" } | { kind: "sign-in" } | { kind: "studio"; session: Session };

export function App() {
  const [view, setView] = useState<View>({ kind: "loading" });
  const [trouble, setTrouble] = useState("");

  useEffect(() => {
    (async () => {
      try {
        const status = await api.status();
        if (status.setup) return setView({ kind: "setup" });
        const session = await api.session();
        setView(session ? { kind: "studio", session } : { kind: "sign-in" });
      } catch (e) {
        setTrouble(e instanceof Error ? e.message : "the studio does not answer");
      }
    })();
  }, []);

  if (trouble) return <Card title="OpenDuct cannot be reached">{trouble}</Card>;
  switch (view.kind) {
    case "loading":
      return null;
    case "setup":
      return <Setup onDone={(session) => setView({ kind: "studio", session })} />;
    case "sign-in":
      return <SignIn onDone={(session) => setView({ kind: "studio", session })} />;
    case "studio":
      return <Studio session={view.session} onSignedOut={() => setView({ kind: "sign-in" })} />;
  }
}

function Card(props: { title: string; lead?: string; children: React.ReactNode }) {
  return (
    <main className="center auth">
      <div className="card auth-card">
        <div className="auth-brand">
          <Wordmark height={64} />
        </div>
        <h1>{props.title}</h1>
        {props.lead && <p className="lead">{props.lead}</p>}
        {props.children}
      </div>
    </main>
  );
}

/** A form that sends once, shows the server's reason when it refuses, and hands the session on. */
function useSend(send: () => Promise<Session>, onDone: (s: Session) => void) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      onDone(await send());
    } catch (e) {
      setError(e instanceof Error ? e.message : "something went wrong");
    } finally {
      setBusy(false);
    }
  };
  return { busy, error, submit };
}

function Setup(props: { onDone: (s: Session) => void }) {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const { busy, error, submit } = useSend(() => api.setup(name, email, password), props.onDone);
  return (
    <Card title="Welcome" lead="Make the owner's account. You sign in with it from now on.">
      <form onSubmit={submit}>
        <label>
          Your name
          <input value={name} onChange={(e) => setName(e.target.value)} autoComplete="name" required autoFocus />
        </label>
        <label>
          E-mail address
          <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} autoComplete="email" required />
        </label>
        <label>
          <span className="label-line">
            Password <span className="hint">at least 10 characters</span>
          </span>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="new-password"
            minLength={10}
            required
          />
        </label>
        {error && <p className="error">{error}</p>}
        <button className="primary" disabled={busy}>
          {busy ? "Making the account…" : "Make the account"}
        </button>
      </form>
    </Card>
  );
}

function SignIn(props: { onDone: (s: Session) => void }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const { busy, error, submit } = useSend(() => api.signIn(email, password), props.onDone);
  return (
    <Card title="Sign in" lead="Welcome back. Sign in to your studio.">
      <form onSubmit={submit}>
        <label>
          E-mail address
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            autoComplete="email"
            required
            autoFocus
          />
        </label>
        <label>
          Password
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="current-password"
            required
          />
        </label>
        {error && <p className="error">{error}</p>}
        <button className="primary" disabled={busy}>
          {busy ? "Signing in…" : "Sign in"}
        </button>
      </form>
    </Card>
  );
}

type Page = "dashboard" | "overview" | "workflows" | "pipelines" | "templates" | "runs" | "logs" | "credentials" | "files" | "members" | "settings";

const PAGE_TITLES: Record<Page, string> = {
  dashboard: "Dashboard",
  overview: "All workspaces",
  workflows: "Workflows",
  pipelines: "Pipelines",
  templates: "Templates",
  runs: "Runs",
  logs: "Logs",
  credentials: "Credentials",
  files: "Files",
  members: "Members",
  settings: "Settings",
};

/** Where the studio is: a page, or one workflow. Kept in the address, so a page can be bookmarked. */
function routeOf(address: string): { page: Page; id: string | null } {
  // A page's own part of the address only: what follows # or ? is the page's to read.
  const path = address.split(/[?#]/)[0] ?? "";
  const workflow = path.match(/^\/workflows\/([A-Za-z0-9-]+)/);
  if (workflow) return { page: "workflows", id: workflow[1] ?? null };
  const page = path.split("/")[1] as Page;
  // The dashboard is home: where the studio opens, and where an address it does not know leads.
  return { page: ["overview", "workflows", "pipelines", "templates", "runs", "logs", "credentials", "files", "members", "settings"].includes(page) ? page : "dashboard", id: null };
}

/** The workspace last shown in this browser, if the user still has it. */
function remembered(session: Session): string {
  let kept = "";
  try {
    kept = localStorage.getItem("openduct.workspace") ?? "";
  } catch {
    kept = "";
  }
  return session.workspaces.some((w) => w.id === kept) ? kept : (session.workspaces[0]?.id ?? "");
}

function Studio(props: { session: Session; onSignedOut: () => void }) {
  const [session, setSession] = useState(props.session);
  const [route, setRoute] = useState(() => routeOf(location.pathname));
  const [workspace, setCurrent] = useState(() => remembered(props.session));
  setWorkspace(workspace);
  useEffect(() => {
    const back = () => setRoute(routeOf(location.pathname));
    addEventListener("popstate", back);
    return () => removeEventListener("popstate", back);
  }, []);
  const go = (path: string) => {
    history.pushState(null, "", path);
    setRoute(routeOf(path));
  };
  const choose = (id: string) => {
    try {
      localStorage.setItem("openduct.workspace", id);
    } catch {
      // A browser that keeps nothing: the choice lasts this visit.
    }
    setCurrent(id);
    // Another workspace: its own tabs, and none of this one's workflows kept.
    setAlive([]);
    go("/dashboard");
  };
  /** A place in another workspace (an alert's, a client's): that workspace, then the place. */
  const openAt = (id: string, path: string) => {
    if (id !== workspace) choose(id);
    go(path);
  };
  const madeWorkspace = async (id: string) => {
    const fresh = await api.session();
    if (fresh) setSession(fresh);
    choose(id);
  };
  const signOut = async () => {
    await api.signOut().catch(() => undefined);
    props.onSignedOut();
  };
  const [chosenFold, setFolded] = useFolded();
  const narrow = useNarrow();
  const folded = chosenFold || narrow;
  const newWorkflow = async () => {
    try {
      const made = await api.create({ name: "New workflow", trigger: { webhook: {} }, steps: [] });
      go(`/workflows/${made.id}`);
    } catch {
      go("/workflows");
    }
  };
  const role = session.workspaces.find((w) => w.id === workspace)?.role ?? "viewer";
  const canEdit = role !== "viewer";
  const tabs = useTabs(workspace);
  const here = route.id ? `/workflows/${route.id}` : `/${route.page}`;
  // Every page and workflow opened stays open as a tab.
  useEffect(() => tabs.open(here, route.id ? "Workflow" : PAGE_TITLES[route.page]), [here, tabs.open]);
  // The workflows opened in this visit, each kept as it is (its changes not yet saved too) while its tab is open.
  const [alive, setAlive] = useState<string[]>([]);
  useEffect(() => {
    if (route.id && !alive.includes(route.id)) setAlive([...alive, route.id]);
  }, [route.id]);
  const open = new Set(tabs.tabs.map((t) => t.path));
  const kept = alive.filter((id) => open.has(`/workflows/${id}`));
  const close = (path: string) => {
    const at = tabs.tabs.findIndex((t) => t.path === path);
    tabs.close(path);
    // As a browser does: the tab to its right opens, else the one to its left.
    if (path === here) go((tabs.tabs[at + 1] ?? tabs.tabs[at - 1])?.path ?? "/workflows");
  };
  const renamed = async () => {
    const fresh = await api.session();
    if (fresh) setSession(fresh);
  };
  return (
    <div className={`shell${folded ? " folded" : ""}`}>
      <Sidebar
        workspaces={session.workspaces}
        workspace={workspace}
        onChoose={choose}
        canMake={session.account.admin}
        installationAdmin={session.account.admin}
        onMade={madeWorkspace}
        account={session.account.name}
        onSignOut={signOut}
        page={route.page}
        inWorkflow={!!route.id}
        admin={role === "admin"}
        canEdit={canEdit}
        folded={folded}
        narrow={narrow}
        onFold={setFolded}
        onGo={go}
        onNewWorkflow={newWorkflow}
      />
      <div className="work">
        <TabBar tabs={tabs.tabs} current={here} onGo={go} onClose={close} end={<Bell key={workspace} onOpenAt={openAt} />} />
          {kept.map((id) => (
            <div key={`${workspace}/${id}`} className="kept" hidden={route.id !== id}>
              <Editor
                id={id}
                active={route.id === id}
                onTitle={(title, dirty) => tabs.name(`/workflows/${id}`, title, dirty)}
                onGone={() => close(`/workflows/${id}`)}
              />
            </div>
          ))}
          {!route.id && page()}
      </div>
    </div>
  );

  function page() {
    return route.page === "credentials" ? (
        <Credentials key={workspace} canEdit={canEdit} hash={location.hash} />
      ) : route.page === "files" ? (
        <Files key={workspace} canEdit={canEdit} />
      ) : route.page === "members" ? (
        <Members key={workspace} workspace={workspace} me={session.account.id} />
      ) : route.page === "pipelines" ? (
        <Pipelines key={workspace} canEdit={canEdit} />
      ) : route.page === "overview" ? (
        <AllWorkspaces current={workspace} onOpenAt={openAt} />
      ) : route.page === "dashboard" ? (
        <Dashboard key={workspace} session={session} workspace={workspace} canEdit={canEdit} onGo={go} onOpenClient={openAt} onNewWorkflow={newWorkflow} />
      ) : route.page === "templates" ? (
        <Templates key={workspace} onOpen={(id) => go(`/workflows/${id}`)} />
      ) : route.page === "runs" ? (
        <Runs key={workspace} onOpen={(id) => go(`/workflows/${id}`)} />
      ) : route.page === "logs" ? (
        <Logs />
      ) : route.page === "settings" ? (
        <Settings
          key={workspace}
          session={session}
          workspace={workspace}
          onRenamed={renamed}
          onGo={go}
          tabs={tabs.tabs.length}
          onCloseTabs={() => tabs.tabs.filter((t) => t.path !== "/dashboard" && t.path !== "/settings").forEach((t) => tabs.close(t.path))}
        />
      ) : (
        <Workflows key={workspace} onOpen={(id) => go(`/workflows/${id}`)} onTemplates={() => go("/templates")} />
      );
  }
}
