package mr.dto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextMultiple implements WritableComparable<TextMultiple> {

    private Text[] texts;

    public TextMultiple() {
        this.texts = new Text[] { new Text() };
    }

    public TextMultiple(String... texts) {
        this.texts = new Text[texts.length];
        for (int i = 0; i < texts.length; i++) {
            this.texts[i] = new Text(texts[i]);
        }
    }

    public TextMultiple(TextMultiple... others) {
        int size = 0;
        for (TextMultiple other : others) {
            size += other.texts.length;
        }
        this.texts = new Text[size];
        int i = 0;
        for (TextMultiple other : others) {
            for (Text ot : other.texts) {
                this.texts[i++] = ot;
            }
        }

    }

    public TextMultiple(TextMultiple other, String... texts) {
        this.texts = new Text[texts.length + other.texts.length];
        int i;
        for (i = 0; i < other.texts.length; i++) {
            this.texts[i] = other.texts[i];
        }
        for (int j = 0; j < texts.length; j++) {
            this.texts[i + j] = new Text(texts[j]);
        }
    }

    public TextMultiple(TextMultiple other, Set<Integer> excludes, String... texts) {
        this.texts = new Text[texts.length + other.texts.length - excludes.size()];
        int j;
        for (j = 0; j < texts.length; j++) {
            this.texts[j] = new Text(texts[j]);
        }
        int it = 0;
        for (int i = 0; i < other.texts.length; i++) {
            if (!excludes.contains(i)) {
                this.texts[j + it++] = other.texts[i];
            }
        }
    }

    public TextMultiple(String[] texts, int[] pos) {
        this.texts = new Text[pos.length];
        for (int i = 0; i < pos.length; i++) {
            this.texts[i] = new Text(texts[pos[i]]);
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
        for (int i = 0; i < texts.length && i < tm.texts.length; i++) {
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
}

