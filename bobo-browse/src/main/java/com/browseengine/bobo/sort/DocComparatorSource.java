package com.browseengine.bobo.sort;

import java.io.IOException;
import java.text.Collator;
import java.util.Locale;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.DocTerms;
import org.apache.lucene.search.FieldCache.DocTermsIndex;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.BytesRef;

public abstract class DocComparatorSource {
	
    boolean _reverse = false;
	
	public DocComparatorSource setReverse(boolean reverse){
		_reverse = reverse;
    return this;
	}
	
	public final boolean isReverse(){
		return _reverse;
	}
	
	public abstract DocComparator getComparator(AtomicReader reader,int docbase)
			throws IOException;

	public static class IntDocComparatorSource extends DocComparatorSource {
		private final String field;

		public IntDocComparatorSource(String field) {
			this.field = field;
		}

		public DocComparator getComparator(AtomicReader reader,int docbase)
				throws IOException {

			final int[] values = FieldCache.DEFAULT.getInts(reader, field,false);

			return new DocComparator() {
				public int compare(ScoreDoc doc1, ScoreDoc doc2) {
					// cannot return v1-v2 because it could overflow
					if (values[doc1.doc] < values[doc2.doc]) {
						return -1;
					} else if (values[doc1.doc] > values[doc2.doc]) {
						return 1;
					} else {
						return 0;
					}
				}

				public Integer value(ScoreDoc doc) {
					return Integer.valueOf(values[doc.doc]);
				}
			};
		}
	}
	
	public static class StringValComparatorSource extends DocComparatorSource {
		private final String field;

		public StringValComparatorSource(String field) {
			this.field = field;
		}

		public DocComparator getComparator(AtomicReader reader,int docbase)
				throws IOException {

			final DocTerms values = FieldCache.DEFAULT.getTerms(reader, field);

			return new DocComparator() {
				public int compare(ScoreDoc doc1, ScoreDoc doc2) {
				    if (!values.exists(doc1.doc)) {
				      if (!values.exists(doc2.doc)) {
				        return 0;
				      }
				      return -1;
				    } else if (!values.exists(doc2.doc)) {
				      return 1;
				    }				   
				    BytesRef bytesRef = new BytesRef();
			      BytesRef bytesRef2 = new BytesRef();
			      
			      bytesRef = values.getTerm(doc1.doc, bytesRef);
			      bytesRef2 = values.getTerm(doc2.doc, bytesRef2);
            return bytesRef.compareTo(bytesRef2);
				}

				public BytesRef value(ScoreDoc doc) {
				  return values.getTerm(doc.doc, new BytesRef());
				}
			};
		}
	}
	
	public static class StringOrdComparatorSource extends DocComparatorSource {
		private final String field;

		public StringOrdComparatorSource(String field) {
			this.field = field;
		}

		public DocComparator getComparator(AtomicReader reader,int docbase)
				throws IOException {

			final DocTermsIndex values = FieldCache.DEFAULT.getTermsIndex(reader, field);

			return new DocComparator() {
				public int compare(ScoreDoc doc1, ScoreDoc doc2) {
				  int ord1 = values.getOrd(doc1.doc);
				  int ord2 = values.getOrd(doc2.doc);
					return ord1 - ord2;
				}

				public BytesRef value(ScoreDoc doc) {
				  int ord = values.getOrd(doc.doc);
				  return values.lookup(ord, new BytesRef());
				}
			};
		}
	}
	
	public static class ShortDocComparatorSource extends DocComparatorSource {
		private final String field;

		public ShortDocComparatorSource(String field) {
			this.field = field;
		}

		public DocComparator getComparator(AtomicReader reader,int docbase)
				throws IOException {

			final short[] values = FieldCache.DEFAULT.getShorts(reader, field, false);

			return new DocComparator() {
				public int compare(ScoreDoc doc1, ScoreDoc doc2) {
					return values[doc1.doc] - values[doc2.doc];
				}

				public Short value(ScoreDoc doc) {
					return Short.valueOf(values[doc.doc]);
				}
			};
		}
	}
	
	public static class LongDocComparatorSource extends DocComparatorSource {
		private final String field;

		public LongDocComparatorSource(String field) {
			this.field = field;
		}

		public DocComparator getComparator(AtomicReader reader,int docbase)
				throws IOException {

			final long[] values = FieldCache.DEFAULT.getLongs(reader, field, false);

			return new DocComparator() {
				public int compare(ScoreDoc doc1, ScoreDoc doc2) {
					// cannot return v1-v2 because it could overflow
					if (values[doc1.doc] < values[doc2.doc]) {
						return -1;
					} else if (values[doc1.doc] > values[doc2.doc]) {
						return 1;
					} else {
						return 0;
					}
				}

				public Long value(ScoreDoc doc) {
					return Long.valueOf(values[doc.doc]);
				}
			};
		}
	}
	
	public static class FloatDocComparatorSource extends DocComparatorSource {
		private final String field;

		public FloatDocComparatorSource(String field) {
			this.field = field;
		}

		public DocComparator getComparator(AtomicReader reader,int docbase)
				throws IOException {

			final float[] values = FieldCache.DEFAULT.getFloats(reader, field, false);

			return new DocComparator() {
				public int compare(ScoreDoc doc1, ScoreDoc doc2) {
					// cannot return v1-v2 because it could overflow
					if (values[doc1.doc] < values[doc2.doc]) {
						return -1;
					} else if (values[doc1.doc] > values[doc2.doc]) {
						return 1;
					} else {
						return 0;
					}
				}

				public Float value(ScoreDoc doc) {
					return Float.valueOf(values[doc.doc]);
				}
			};
		}
	}
	
	public static class DoubleDocComparatorSource extends DocComparatorSource {
		private final String field;

		public DoubleDocComparatorSource(String field) {
			this.field = field;
		}

		public DocComparator getComparator(AtomicReader reader,int docbase)
				throws IOException {

			final double[] values = FieldCache.DEFAULT.getDoubles(reader, field, false);

			return new DocComparator() {
				public int compare(ScoreDoc doc1, ScoreDoc doc2) {
					// cannot return v1-v2 because it could overflow
					if (values[doc1.doc] < values[doc2.doc]) {
						return -1;
					} else if (values[doc1.doc] > values[doc2.doc]) {
						return 1;
					} else {
						return 0;
					}
				}

				public Double value(ScoreDoc doc) {
					return Double.valueOf(values[doc.doc]);
				}
			};
		}
	}
	
	public static class RelevanceDocComparatorSource extends DocComparatorSource {
		public RelevanceDocComparatorSource() {
		}

		public DocComparator getComparator(AtomicReader reader,int docbase)
				throws IOException {

			return new DocComparator() {
				public int compare(ScoreDoc doc1, ScoreDoc doc2) {
					// cannot return v1-v2 because it could overflow
					if (doc1.score < doc2.score) {
						return -1;
					} else if (doc1.score > doc2.score) {
						return 1;
					} else {
						return 0;
					}
				}

				public Float value(ScoreDoc doc) {
					return Float.valueOf(doc.score);
				}
			};
		}
		
		
	}
	
	public static class DocIdDocComparatorSource extends DocComparatorSource {
		public DocIdDocComparatorSource() {
		}

		public DocComparator getComparator(AtomicReader reader,int docbase)
				throws IOException {

			final int _docbase = docbase;

			return new DocComparator() {
				public int compare(ScoreDoc doc1, ScoreDoc doc2) {
					return doc1.doc-doc2.doc;
				}

				public Integer value(ScoreDoc doc) {
					return Integer.valueOf(doc.doc+_docbase);
				}
			};
		}
	}
	
	public static class ByteDocComparatorSource extends DocComparatorSource {
		private final String field;

		public ByteDocComparatorSource(String field) {
			this.field = field;
		}

		public DocComparator getComparator(AtomicReader reader,int docbase)
				throws IOException {

			final byte[] values = FieldCache.DEFAULT.getBytes(reader, field,false);

			return new DocComparator() {
				public int compare(ScoreDoc doc1, ScoreDoc doc2) {
					return values[doc1.doc] - values[doc2.doc];
				}

				public Byte value(ScoreDoc doc) {
					return Byte.valueOf(values[doc.doc]);
				}
			};
		}
	}
	
	
}
