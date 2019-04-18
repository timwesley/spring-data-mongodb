/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 * @since 2.2
 */
public class StringBasedAggregation extends AbstractMongoQuery {

	private static final ParameterBindingDocumentCodec CODEC = new ParameterBindingDocumentCodec();

	private final MongoOperations mongoOperations;
	private final MongoConverter mongoConverter;
	private final SpelExpressionParser expressionParser;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;

	/**
	 * Creates a new {@link StringBasedAggregation} from the given {@link MongoQueryMethod} and {@link MongoOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param expressionParser
	 * @param evaluationContextProvider
	 */
	public StringBasedAggregation(MongoQueryMethod method, MongoOperations mongoOperations,
			SpelExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {
		super(method, mongoOperations);

		this.mongoOperations = mongoOperations;
		this.mongoConverter = mongoOperations.getConverter();
		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;
	}

	@Override
	protected Object doExecute(MongoQueryMethod method, ConvertingParameterAccessor accessor, Class<?> typeToRead) {

		Class<?> sourceType = method.repositoryDomainType();
		Class<?> targetType = typeToRead;

		List<AggregationOperation> pipeline = computePipeline(method, accessor);
		appendSortIfPresent(pipeline, accessor, typeToRead);
		appendLimitAndOffsetIfPresent(pipeline, accessor);

		boolean isSimpleReturnType = isSimpleReturnType(typeToRead);
		boolean isRawAggregationResult = ClassUtils.isAssignable(AggregationResults.class, typeToRead);

		if (isSimpleReturnType) {
			targetType = Document.class;
		} else if (isRawAggregationResult) {

			targetType = method.getReturnType().getActualType().getComponentType().getType();
		}

		// TODO: add collation!!
		AggregationOptions options = Aggregation.newAggregationOptions().build();
		AggregationResults<?> result = mongoOperations.aggregate(new TypedAggregation<>(sourceType, pipeline, options),
				targetType);

		if (isRawAggregationResult) {
			return result;
		}

		if (method.isCollectionQuery()) {

			if (isSimpleReturnType) {
				return result.getMappedResults().stream().map(it -> extractSimpleTypeResult((Document) it, typeToRead))
						.collect(Collectors.toList());
			}

			return result.getMappedResults();
		}

		if (isSimpleReturnType) {
			return extractSimpleTypeResult((Document) result.getUniqueMappedResult(), typeToRead);
		}

		return result.getUniqueMappedResult();
	}

	private boolean isSimpleReturnType(Class<?> targetType) {
		return MongoSimpleTypes.HOLDER.isSimpleType(targetType);
	}

	@Nullable
	private Object extractSimpleTypeResult(Document source, Class<?> targetType) {

		if (source.isEmpty()) {
			return null;
		}

		if (source.size() == 1) {
			return getPotentiallyConvertedSimpleTypeValue(source.values().iterator().next(), targetType);
		}

		Document tmp = new Document(source);
		tmp.remove("_id");

		if (tmp.size() == 1) {
			return getPotentiallyConvertedSimpleTypeValue(tmp.values().iterator().next(), targetType);
		}

		for (Map.Entry<String, Object> entry : tmp.entrySet()) {
			if (entry != null && ClassUtils.isAssignable(targetType, entry.getValue().getClass())) {
				return entry.getValue();
			}
		}

		throw new IllegalStateException(
				String.format("o_O no entry of type %s found in %s.", targetType.getSimpleName(), source.toJson()));
	}

	@Nullable
	private Object getPotentiallyConvertedSimpleTypeValue(Object value, Class<?> targetType) {

		if (value == null) {
			return value;
		}

		if (!mongoConverter.getConversionService().canConvert(value.getClass(), targetType)) {
			return value;
		}

		return mongoConverter.getConversionService().convert(value, targetType);
	}

	List<AggregationOperation> computePipeline(MongoQueryMethod method, ConvertingParameterAccessor accessor) {

		ParameterBindingContext bindingContext = new ParameterBindingContext((accessor::getBindableValue), expressionParser,
				evaluationContextProvider.getEvaluationContext(getQueryMethod().getParameters(), accessor.getValues()));

		List<AggregationOperation> target = new ArrayList<>();

		for (String source : method.getAnnotatedAggregation()) {
			target.add(ctx -> CODEC.decode(source, bindingContext));
		}

		return target;
	}

	private void appendSortIfPresent(List<AggregationOperation> aggregationPipeline, ConvertingParameterAccessor accessor,
			Class<?> targetType) {

		if (accessor.getSort().isUnsorted()) {
			return;
		}

		aggregationPipeline.add(ctx -> {

			Document sort = new Document();
			for (Order order : accessor.getSort()) {
				sort.append(order.getProperty(), order.isAscending() ? 1 : -1);
			}

			return ctx.getMappedObject(new Document("$sort", sort), targetType);
		});
	}

	private void appendLimitAndOffsetIfPresent(List<AggregationOperation> aggregationPipeline,
			ConvertingParameterAccessor accessor) {

		Pageable pageable = accessor.getPageable();
		if (pageable.isUnpaged()) {
			return;
		}

		if (pageable.getOffset() > 0) {
			aggregationPipeline.add(Aggregation.skip(pageable.getOffset()));
		}

		aggregationPipeline.add(Aggregation.limit(pageable.getPageSize()));
	}

	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {
		return new BasicQuery("{}");
	}

	@Override
	protected boolean isCountQuery() {
		return false;
	}

	@Override
	protected boolean isExistsQuery() {
		return false;
	}

	@Override
	protected boolean isDeleteQuery() {
		return false;
	}

	@Override
	protected boolean isLimiting() {
		return false;
	}
}
