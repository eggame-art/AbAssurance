import type { MouseEvent } from "react";
import styles from "./Navigation.module.css";

type NavigationProps = {
  isDataPage: boolean;
  onNavigate: (path: string) => void;
};

export function Navigation({ isDataPage, onNavigate }: NavigationProps) {
  function follow(event: MouseEvent<HTMLAnchorElement>) {
    if (event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    onNavigate(event.currentTarget.pathname);
  }
  return (
    <nav aria-label="Navigation principale" className={styles.navigation}>
      <a href="/pipeline" onClick={follow} aria-current={!isDataPage ? "page" : undefined}>
        Pipeline de données
      </a>
      <a href="/donnees" onClick={follow} aria-current={isDataPage ? "page" : undefined}>
        Données et prédictions
      </a>
    </nav>
  );
}
