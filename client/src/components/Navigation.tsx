import styles from "./Navigation.module.css";

type NavigationProps = {
  isDataPage: boolean;
};

export function Navigation({ isDataPage }: NavigationProps) {
  return (
    <nav aria-label="Navigation principale" className={styles.navigation}>
      <a href="#/pipeline" aria-current={!isDataPage ? "page" : undefined}>
        Pipeline de données
      </a>
      <a href="#/donnees" aria-current={isDataPage ? "page" : undefined}>
        Données et prédictions
      </a>
    </nav>
  );
}
