/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
 ...
***********************************************************************/
package org.datanucleus.store.xml.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.inmemory.JPQLInMemoryEvaluator;
import org.datanucleus.query.inmemory.JavaQueryInMemoryEvaluator;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Implementation of JPQL for XML datastores.
 */
public class JPQLQuery extends AbstractJPQLQuery
{
    private static final long serialVersionUID = -5069640831833631716L;

    /**
     * Constructs a new query instance that uses the given persistence manager.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (JPQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param q The query from which to copy criteria.
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, JPQLQuery q)
    {
        super(storeMgr, ec, q);
    }

    /**
     * Constructor for a JPQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param query The query string
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec, query);
    }

    protected Object performExecute(Map parameters)
    {
        ManagedConnection mconn = getStoreManager().getConnectionManager().getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021046", Query.LANGUAGE_JPQL, getSingleStringQuery(), null));
            }
            List candidates = null;
            if (candidateCollection == null)
            {
                candidates = new XMLCandidateList(candidateClass, subclasses, ec,  (String)getExtension(Query.EXTENSION_RESULT_CACHE_TYPE), mconn, ignoreCache);
            }
            else
            {
                candidates = new ArrayList(candidateCollection);
            }

            // Apply any result restrictions to the XML XPath results
            JavaQueryInMemoryEvaluator resultMapper = new JPQLInMemoryEvaluator(this, candidates, compilation, 
                parameters, ec.getClassLoaderResolver());
            Collection results = resultMapper.execute(true, true, true, true, true);

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021074", Query.LANGUAGE_JPQL, 
                    "" + (System.currentTimeMillis() - startTime)));
            }

            if (type == QueryType.BULK_DELETE)
            {
                ec.deleteObjects(results.toArray());
                return Long.valueOf(results.size());
            }
            else if (type == QueryType.BULK_UPDATE)
            {
                throw new NucleusException("Bulk Update is not yet supported");
            }
            else
            {
                return results;
            }
        }
        finally
        {
            mconn.release();
        }
    }
}