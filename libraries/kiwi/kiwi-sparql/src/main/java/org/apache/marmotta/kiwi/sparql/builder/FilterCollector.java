/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.marmotta.kiwi.sparql.builder;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.ArrayList;
import java.util.List;

/**
* Collect all filter conditions in a query expression
*
* @author Sebastian Schaffert (sschaffert@apache.org)
*/
public class FilterCollector extends QueryModelVisitorBase<RuntimeException> {

    List<ValueExpr> filters = new ArrayList<>();

    public FilterCollector(TupleExpr expr) {
        expr.visit(this);
    }

    @Override
    public void meet(Filter node) throws RuntimeException {
        filters.add(node.getCondition());

        super.meet(node);
    }
}