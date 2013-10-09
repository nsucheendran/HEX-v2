package mr.dto;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class TextMultiple implements WritableComparable<TextMultiple> {

    private Text[] texts;

    public TextMultiple() {
        this.texts = new Text[] { new Text() };
    }

    public TextMultiple(String... texts) {
        this.texts = new Text[texts.length];
        for ( int i = 0; i < texts.length; i++ ) {
            this.texts[i] = new Text( texts[i] );
        }
    }

    public Text getTextElementAt(int position) {
        return texts[position];
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(texts.length);
        for ( Text text : texts ) {
            text.write( out );
        }
    }

    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        if ( texts.length < length ) {
            texts = Arrays.copyOf( texts, length );
        }
        for ( int i = 0; i < length; i++ ) {
            Text text = texts[i];
            if ( text == null ) {
                text = new Text();
                texts[i] = text;
            }
            text.readFields( in );
        }
    }

    public int compareTo(TextMultiple tm) {
        for ( int i = 0; i < texts.length && i < tm.texts.length; i++ ) {
            int cmp = texts[i].compareTo( tm.texts[i] );
            if ( cmp != 0 ) {
                return cmp;
            }
        }
        return ( texts.length == tm.texts.length ) ? 0 : ( texts.length > tm.texts.length ) ? 1 : -1;
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
        for ( int i = 0; i < texts.length; i++ ) {
            Text text = texts[i];
            strBuff.append( text );
            if ( i != texts.length - 1 ) {
                strBuff.append( "\t" );
            }
        }
        return strBuff.toString();
    }
}
