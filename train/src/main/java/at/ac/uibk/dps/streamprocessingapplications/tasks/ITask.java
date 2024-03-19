package at.ac.uibk.dps.streamprocessingapplications.tasks;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public interface ITask<T, U> {
    public void setup(Logger l_, Properties p_) throws IOException;

    /**
     * Task logic that will be executed on each input event seen.
     *
     * @param map the input message
     * @return null if there is no message output, Float.MIN_FLOAT if there is an error, output from
     * the logic otherwise.
     */
    public Float doTask(Map<String, T> map) throws IOException;

    /**
     * Returns the last result that was set by the child task in case there is a result other than
     * the float returned by doTaskLogic()
     *
     * @return
     */
    public U getLastResult();

    public float tearDown();
}
