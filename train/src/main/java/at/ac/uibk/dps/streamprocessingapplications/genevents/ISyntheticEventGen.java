package at.ac.uibk.dps.streamprocessingapplications.genevents;

import java.util.List;

public interface ISyntheticEventGen {
  public void receive(List<String> event); // event
}
