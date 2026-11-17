import { useState } from "react";
import { authController } from "../controllers/authController";
import type { User } from "../models/User";
import styles from "./LoginPage.module.css";

export function LoginPage({ onLogin }: { onLogin: (user: User) => void }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function login() {
    setLoading(true);
    setError("");
    try {
      onLogin(await authController.login(username, password));
      setPassword("");
    } catch (error) {
      setError(
        error instanceof Error ? error.message : "Connexion impossible.",
      );
    } finally {
      setLoading(false);
    }
  }

  return (
    <section>
      <h1>Connexion</h1>
      <form
        className={styles.form}
        onSubmit={(event) => {
          event.preventDefault();
          void login();
        }}
      >
        <label>
          Identifiant
          <input
            autoComplete="username"
            required
            maxLength={100}
            value={username}
            onChange={(event) => setUsername(event.target.value)}
            disabled={loading}
          />
        </label>
        <label>
          Mot de passe
          <input
            type="password"
            autoComplete="current-password"
            required
            maxLength={1024}
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            disabled={loading}
          />
        </label>
        <button type="submit" disabled={loading}>
          {loading ? "Connexion..." : "Se connecter"}
        </button>
      </form>
      {error && <p role="alert">{error}</p>}
    </section>
  );
}
