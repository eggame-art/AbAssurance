import { useEffect, useState } from "react";
import { Navigation } from "./components/Navigation";
import { DataPage } from "./systems/data/pages/DataPage";
import { PipelinePage } from "./systems/integration/pages/PipelinePage";
import styles from "./App.module.css";

export function App() {
  const [hash, setHash] = useState(() => window.location.hash);
  const isDataPage = hash === "#/donnees";

  useEffect(() => {
    const handleHashChange = () => setHash(window.location.hash);

    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  return (
    <div className={styles.container}>
      <header className={styles.header}>
        <p className={styles.name}>AbAssurance</p>
        <Navigation isDataPage={isDataPage} />
      </header>

      <main>{isDataPage ? <DataPage /> : <PipelinePage />}</main>
    </div>
  );
}
