import { pipelineService } from "../services/pipelineService";

export const pipelineController = {
  getServicesStatus: pipelineService.getServicesStatus,
  getLatestIntegration: pipelineService.getLatestIntegration,
  startIntegration: pipelineService.startIntegration,
};
