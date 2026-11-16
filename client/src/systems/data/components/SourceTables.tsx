import type { ClientsResponse } from "../models/Client";
import type { ContractsResponse } from "../models/Contract";
import type { ClaimsResponse } from "../models/Claim";
import type { PaymentsResponse } from "../models/Payment";
import styles from "./SourceTables.module.css";

export function ClientsTable({ data }: { data: ClientsResponse }) {
  return (
    <div className={styles.container}>
      <table
        className={styles.table}
        aria-label={`Clients ${data.source === "oracle" ? "AbAssurance" : "AssurePlus"}`}
      >
        <thead>
          <tr>
            <th scope="col">Identifiant source</th>
            <th scope="col">
              {data.source === "oracle" ? "Nom" : "Nom complet"}
            </th>
            {data.source === "oracle" && <th scope="col">Prénom</th>}
            <th scope="col">E-mail</th>
            <th scope="col">Téléphone</th>
            <th scope="col">Statut</th>
          </tr>
        </thead>
        <tbody>
          {data.clients.map((client) => (
            <tr key={`${data.source}:${client.source_id}`}>
              <td>{client.source_id}</td>
              <td>{client.name}</td>
              {data.source === "oracle" && <td>{client.first_name}</td>}
              <td>{client.email}</td>
              <td>{client.phone}</td>
              <td>{client.status}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function ContractsTable({ data }: { data: ContractsResponse }) {
  return (
    <div className={styles.container}>
      <table
        className={styles.table}
        aria-label={`Contrats ${data.source === "oracle" ? "AbAssurance" : "AssurePlus"}`}
      >
        <thead>
          <tr>
            <th scope="col">Référence source</th>
            <th scope="col">Identifiant client source</th>
            <th scope="col">Type</th>
            <th scope="col">Début</th>
            <th scope="col">Fin</th>
            <th scope="col">
              {data.source === "oracle" ? "Prime annuelle" : "Prime mensuelle"}
            </th>
            <th scope="col">Statut</th>
          </tr>
        </thead>
        <tbody>
          {data.contracts.map((contract) => (
            <tr key={`${data.source}:${contract.source_reference}`}>
              <td>{contract.source_reference}</td>
              <td>{contract.source_client_id}</td>
              <td>{contract.product_type}</td>
              <td>{contract.start_date}</td>
              <td>{contract.end_date}</td>
              <td>{contract.premium}</td>
              <td>{contract.status}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function ClaimsTable({ data }: { data: ClaimsResponse }) {
  return (
    <div className={styles.container}>
      <table
        className={styles.table}
        aria-label={`Sinistres ${data.source === "oracle" ? "AbAssurance" : "AssurePlus"}`}
      >
        <thead>
          <tr>
            <th scope="col">Identifiant source</th>
            <th scope="col">Référence contrat source</th>
            <th scope="col">Date du sinistre</th>
            <th scope="col">Montant estimé</th>
            <th scope="col">Statut</th>
            <th scope="col">Description</th>
          </tr>
        </thead>
        <tbody>
          {data.claims.map((claim) => (
            <tr key={`${data.source}:${claim.source_id}`}>
              <td>{claim.source_id}</td>
              <td>{claim.source_contract_reference}</td>
              <td>{claim.incident_date}</td>
              <td>{claim.estimated_amount}</td>
              <td>{claim.status}</td>
              <td className={styles.description}>{claim.description}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function PaymentsTable({ data }: { data: PaymentsResponse }) {
  return (
    <div className={styles.container}>
      <table
        className={styles.table}
        aria-label={`Paiements ${data.source === "oracle" ? "AbAssurance" : "AssurePlus"}`}
      >
        <thead>
          <tr>
            <th scope="col">Identifiant source</th>
            <th scope="col">Référence contrat source</th>
            <th scope="col">Date du paiement</th>
            <th scope="col">Montant</th>
            <th scope="col">Mode</th>
            {data.source === "sqlserver" && <th scope="col">Statut</th>}
          </tr>
        </thead>
        <tbody>
          {data.payments.map((payment) => (
            <tr key={`${data.source}:${payment.source_id}`}>
              <td>{payment.source_id}</td>
              <td>{payment.source_contract_reference}</td>
              <td>{payment.payment_date}</td>
              <td>{payment.amount}</td>
              <td>{payment.method}</td>
              {data.source === "sqlserver" && <td>{payment.status}</td>}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
