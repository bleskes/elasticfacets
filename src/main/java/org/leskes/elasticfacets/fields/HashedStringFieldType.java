package org.leskes.elasticfacets.fields;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.ints.IntFieldDataComparator;
import org.elasticsearch.index.field.data.ints.IntFieldDataMissingComparator;

// Hashed strings are just integers - code is copied from there

public class HashedStringFieldType implements
		FieldDataType<HashedStringFieldData> {

	public static int hashCode(String s) {
		// place holder so we can replace hashing later on...
		return s.hashCode();
	}

	public FieldDataType.ExtendedFieldComparatorSource newFieldComparatorSource(
			final FieldDataCache cache, @Nullable final String missing) {
		if (missing == null) {
			return new ExtendedFieldComparatorSource() {
				@Override
				public FieldComparator newComparator(String fieldname,
						int numHits, int sortPos, boolean reversed)
						throws IOException {
					return new IntFieldDataComparator(numHits, fieldname, cache);
				}

				@Override
				public int reducedType() {
					return SortField.INT;
				}
			};
		}
		if (missing.equals("_last")) {
			return new ExtendedFieldComparatorSource() {
				@Override
				public FieldComparator newComparator(String fieldname,
						int numHits, int sortPos, boolean reversed)
						throws IOException {
					return new IntFieldDataMissingComparator(numHits,
							fieldname, cache, reversed ? Integer.MIN_VALUE
									: Integer.MAX_VALUE);
				}

				@Override
				public int reducedType() {
					return SortField.INT;
				}
			};
		}
		if (missing.equals("_first")) {
			return new ExtendedFieldComparatorSource() {
				@Override
				public FieldComparator newComparator(String fieldname,
						int numHits, int sortPos, boolean reversed)
						throws IOException {
					return new IntFieldDataMissingComparator(numHits,
							fieldname, cache, reversed ? Integer.MAX_VALUE
									: Integer.MIN_VALUE);
				}

				@Override
				public int reducedType() {
					return SortField.INT;
				}
			};
		}
		return new ExtendedFieldComparatorSource() {
			@Override
			public FieldComparator newComparator(String fieldname, int numHits,
					int sortPos, boolean reversed) throws IOException {
				return new IntFieldDataMissingComparator(numHits, fieldname,
						cache, Integer.parseInt(missing));
			}

			@Override
			public int reducedType() {
				return SortField.INT;
			}
		};
	}

	public HashedStringFieldData load(IndexReader reader, String fieldName)
			throws IOException {

		return HashedStringFieldData.load(reader, fieldName);
	}

}
