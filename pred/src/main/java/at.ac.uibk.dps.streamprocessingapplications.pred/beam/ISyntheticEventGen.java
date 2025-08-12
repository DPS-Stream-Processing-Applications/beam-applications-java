package at.ac.uibk.dps.streamprocessingapplications.pred.beam;

import java.util.List;

public interface ISyntheticEventGen {
  public void receive(List<String> event);
}
