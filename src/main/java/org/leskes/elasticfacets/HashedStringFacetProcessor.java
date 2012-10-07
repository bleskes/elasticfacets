package org.leskes.elasticfacets;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.internal.SearchContext;
import org.leskes.elasticfacets.fields.HashedStringFieldType;

public class HashedStringFacetProcessor extends AbstractComponent implements
		FacetProcessor {

	final static ESLogger logger = Loggers
			.getLogger(HashedStringFacetProcessor.class);

	@Inject
	public HashedStringFacetProcessor(Settings settings) {
		super(settings);
	}


	public FacetCollector parse(String facetName, XContentParser parser,
			SearchContext context) throws IOException {

        String field = null;
        int size = 10;
        boolean allTerms = false;

        ImmutableSet<Integer> excluded = ImmutableSet.of();
        TermsFacet.ComparatorType comparatorType = TermsFacet.ComparatorType.COUNT;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("exclude".equals(currentFieldName)) {
                    ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        builder.add(HashedStringFieldType.hashCode(parser.text()));
                    }
                    excluded = builder.build();
                }
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("size".equals(currentFieldName)) {
                    size = parser.intValue();
                } else if ("all_terms".equals(currentFieldName) || "allTerms".equals(currentFieldName)) {
                    allTerms = parser.booleanValue();
                } else if ("order".equals(currentFieldName) || "comparator".equals(currentFieldName)) {
                    comparatorType = TermsFacet.ComparatorType.fromString(parser.text());
                }
            }
        }

        return new HashedStringFacetCollector(facetName, field, size, comparatorType,allTerms,excluded, context);
    }

	public String[] types() {
		return new String[] { "hashed_terms" };
	}


	public Facet reduce(String name, List<Facet> facets) {
		throw new RuntimeException("HashedStringFacets uses the String facet infrastructure. This should never be seen. ");
	}


}
