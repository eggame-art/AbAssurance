import { predictionService } from "../services/predictionService";

export const predictionController = {
  predictOwnContract: predictionService.predictOwnContract,
  predictContract: predictionService.predictContract,
  getModel: predictionService.getModel,
  predict: predictionService.predict,
};
