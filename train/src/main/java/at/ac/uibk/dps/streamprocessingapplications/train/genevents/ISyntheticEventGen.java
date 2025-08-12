package at.ac.uibk.dps.streamprocessingapplications.train.genevents;

import java.util.List;

public interface ISyntheticEventGen {
  public void receive(List<String> event); // event
}
