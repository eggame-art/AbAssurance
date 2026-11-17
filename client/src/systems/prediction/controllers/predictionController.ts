import { predictionService } from "../services/predictionService";

export const predictionController = {
  predictContract: predictionService.predictContract,
  getModel: predictionService.getModel,
  predict: predictionService.predict,
};
