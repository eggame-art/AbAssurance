import { dataService } from "../services/dataService";

export const dataController = {
  getClients: dataService.getClients,
  getContracts: dataService.getContracts,
  getClaims: dataService.getClaims,
  getPayments: dataService.getPayments,
  getConsolidated: dataService.getConsolidated,
};
