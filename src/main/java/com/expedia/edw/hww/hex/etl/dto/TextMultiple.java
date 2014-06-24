package com.expedia.edw.hww.hex.etl.dto;

import static com.expedia.edw.hww.hex.etl.utils.Utils.coalesce;
import static com.expedia.edw.hww.hex.etl.utils.Utils.containsArrayInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import mr.segmentation.SegmentationSpec;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextMultiple implements WritableComparable<TextMultiple> {

  private Text[] texts;

  public TextMultiple() {
    texts = new Text[0];
  }

  public TextMultiple(String... texts) {
    this.texts = new Text[texts.length];
    for (int i = 0; i < texts.length; i++) {
      this.texts[i] = new Text(coalesce(texts[i], ""));
    }
  }

  public int size() {
    return texts.length;
  }

  public void stripeFlank(String[] texts, SegmentationSpec spec, String... appendables) {
    int i = 0;
    i = spec.setSpec(i, this.texts);
    i = spec.setVals(i, texts, this.texts);
    if (appendables != null) {
      for (String val : appendables) {
        this.texts[i++].set(val);
      }
    }
  }

  public void stripeAppend(String[] texts, int[] pos, String... appendables) {
    int i = 0;
    for (int p : pos) {
      if (p < 0) {
        this.texts[i++].set("");
      } else {
        this.texts[i++].set(texts[p]);
      }
    }
    for (String val : appendables) {
      this.texts[i++].set(val);
    }
  }

  public Text getTextElementAt(int position) {
    return texts[position];
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(texts.length);
    for (Text text : texts) {
      text.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    if (texts.length < length) {
      texts = Arrays.copyOf(texts, length);
    }
    for (int i = 0; i < length; i++) {
      Text text = texts[i];
      if (text == null) {
        text = new Text();
        texts[i] = text;
      }
      text.readFields(in);
    }
  }

  @Override
  public int compareTo(TextMultiple tm) {
    int minLen = Math.min(texts.length, tm.texts.length);
    for (int i = 0; i < minLen; i++) {
      int cmp = texts[i].compareTo(tm.texts[i]);
      if (cmp != 0) {
        return cmp;
      }
    }
    return (texts.length == tm.texts.length) ? 0 : (texts.length > tm.texts.length) ? 1 : -1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TextMultiple)) {
      return false;
    }

    TextMultiple that = (TextMultiple) o;

    return Arrays.equals(texts, that.texts);
  }

  @Override
  public int hashCode() {
    return texts != null ? Arrays.hashCode(texts) : 0;
  }

  @Override
  public String toString() {
    StringBuilder strBuff = new StringBuilder();
    for (int i = 0; i < texts.length; i++) {
      Text text = texts[i];
      strBuff.append(text);
      if (i != texts.length - 1) {
        strBuff.append((char) 1);
      }
    }
    return strBuff.toString();
  }

  private static final char SEP = (char) 1;

  public void toStringBuilder(StringBuilder sb, int... excludePos) {
    boolean appended = false;
    for (int i = 0; i < texts.length; i++) {
      if (!containsArrayInt(excludePos, i)) {
        if (appended) {
          sb.append(SEP);
        }
        sb.append(texts[i]);
        appended = true;
      }
    }

  }

  public void toStringBuilderSep(StringBuilder sb, char sep, int... excludePos) {
    boolean appended = false;
    for (int i = 0; i < texts.length; i++) {
      if (!containsArrayInt(excludePos, i)) {
        if (appended) {
          sb.append(sep);
        }
        sb.append(texts[i]);
        appended = true;
      }
    }

  }
}
