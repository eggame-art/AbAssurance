import { useEffect, useState } from "react";
import { Navigation } from "./components/Navigation";
import { DataPage } from "./systems/data/pages/DataPage";
import { PipelinePage } from "./systems/integration/pages/PipelinePage";
import { LoginPage } from "./systems/auth/pages/LoginPage";
import { PersonalPage } from "./systems/auth/pages/PersonalPage";
import { authController } from "./systems/auth/controllers/authController";
import type { User } from "./systems/auth/models/User";
import styles from "./App.module.css";

export function App() {
  const [pathname, setPathname] = useState(() => window.location.pathname);
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const isDataPage = pathname === "/donnees";

  useEffect(() => {
    let active = true;

    function handleLocationChange() {
      const legacyPath = window.location.hash.slice(1);
      if (legacyPath === "/pipeline" || legacyPath === "/donnees") {
        window.history.replaceState(null, "", legacyPath + window.location.search);
      }
      setPathname(window.location.pathname);
    }

    handleLocationChange();

    function handleSessionExpired() {
      setUser(null);
    }

    async function loadSession() {
      try {
        const session = await authController.getSession();

        if (active) setUser(session);
      } catch {
        if (active) setUser(null);
      } finally {
        if (active) setLoading(false);
      }
    }

    window.addEventListener("popstate", handleLocationChange);
    window.addEventListener("hashchange", handleLocationChange);
    window.addEventListener("session-expired", handleSessionExpired);

    void loadSession();

    return () => {
      active = false;

      window.removeEventListener("popstate", handleLocationChange);
      window.removeEventListener("hashchange", handleLocationChange);
      window.removeEventListener("session-expired", handleSessionExpired);
    };
  }, []);

  function navigate(path: string) {
    window.history.pushState(null, "", path);
    setPathname(path);
  }

  async function logout() {
    setError("");

    try {
      await authController.logout();
      setUser(null);
    } catch {
      setError("Impossible de se déconnecter.");
    }
  }

  return (
    <div className={styles.container}>
      <header className={styles.header}>
        <p className={styles.name}>AbAssurance</p>

        {user?.role === "admin" && <Navigation isDataPage={isDataPage} onNavigate={navigate} />}

        {user && (
          <p>
            {user.username} ({user.role}){" "}
            <button type="button" onClick={() => void logout()}>
              Se déconnecter
            </button>
          </p>
        )}
      </header>

      <main>
        {error && <p role="alert">{error}</p>}

        {loading ? (
          <p role="status">Chargement de la session…</p>
        ) : !user ? (
          <LoginPage onLogin={setUser} />
        ) : user.role === "user" ? (
          <PersonalPage />
        ) : isDataPage ? (
          <DataPage />
        ) : (
          <PipelinePage />
        )}
      </main>
    </div>
  );
}
