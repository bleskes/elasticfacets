package org.elasticsearch.plugin.elasticfacets;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
import org.elasticsearch.search.facet.statistical.ScriptStatisticalFacetCollector;
import org.elasticsearch.search.facet.statistical.StatisticalFacetCollector;
import org.elasticsearch.search.facet.statistical.StatisticalFieldsFacetCollector;
import org.elasticsearch.search.facet.terms.InternalTermsFacet;
import org.elasticsearch.search.internal.SearchContext;

public class LetterCountFacetProcessor extends AbstractComponent implements
		FacetProcessor {

	final static ESLogger logger = Loggers
			.getLogger(LetterCountFacetProcessor.class);

	@Inject
	public LetterCountFacetProcessor(Settings settings) {
		super(settings);
		LetterCountFacet.registerStreams();
	}

	public String[] types() {
		return new String[] { LetterCountFacet.TYPE };
	}

	public FacetCollector parse(String facetName, XContentParser parser,
			SearchContext context) throws IOException {

		String field = null;
		String[] fieldsNames = null;
		char C = '\0';

		String currentFieldName = null;
		XContentParser.Token token;
		while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
			if (token == XContentParser.Token.FIELD_NAME) {
				currentFieldName = parser.currentName();
			} else if (token.isValue()) {
				if ("field".equals(currentFieldName)) {
					field = parser.text();
				} else if ("letter".equals(currentFieldName)) {
					C = parser.textCharacters()[0];
				}
			}
		}
		
		return new LetterCountFacetCollector(facetName, field, C, context);
	}

	public Facet reduce(String name, List<Facet> facets) {

		return ((LetterCountFacet) facets.get(0)).reduce(name, facets);
	}

}
