import { predictionService } from "../services/predictionService";

export const predictionController = {
  getModel: predictionService.getModel,
  predict: predictionService.predict,
};
