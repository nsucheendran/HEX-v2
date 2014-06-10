/*
 * @author achadha
 */

package mr.segmentation;

public class ColumnMapping {
  private final int position;
  private final String defaultValue;

  public ColumnMapping(final int pos, final String val) {
    this.position = pos;
    this.defaultValue = val;
  }

  public int position() {
    return position;
  }

  public String defaultValue() {
    return defaultValue;
  }

  @Override
  public String toString() {
    return position + "\t" + defaultValue;
  }

}
