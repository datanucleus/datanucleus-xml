/**********************************************************************
Copyright (c) 2008 Erik Bengtson and others. All rights reserved.
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
2008 Andy Jefferson - abstracted methods to AbstractStoreManager
2008 Andy Jefferson - checks on JAXB jars
 ...
***********************************************************************/
package org.datanucleus.store.xml;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataListener;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.valuegenerator.AbstractConnectedGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationConnectionProvider;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.store.xml.query.JDOQLQuery;
import org.datanucleus.store.xml.query.JPQLQuery;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.w3c.dom.Document;

/**
 * Manager for XML datastores.
 * Relies on JAXB API and an implementation of JAXB to be present.
 */
public class XMLStoreManager extends AbstractStoreManager
{
    static
    {
        Localiser.registerBundle("org.datanucleus.store.xml.Localisation", XMLStoreManager.class.getClassLoader());
    }

    public static final String JAXB_HANDLER_CLASS_PROPERTY = "datanucleus.xml.jaxbhandlerclass";
    public static final String XML_INDENT_SIZE_PROPERTY = "datanucleus.xml.indentsize";

    JAXBHandler jaxbHandler;
    MetaDataListener metadataListener;

    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param ctx Context
     * @param props Properties for this store manager
     */
    public XMLStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext ctx, Map<String, Object> props)
    {
        super("xml", clr, ctx, props);

        ClassUtils.assertClassForJarExistsInClasspath(clr, "javax.xml.bind.JAXBContext", "jaxb-api.jar");

        String jaxbHandlerClassName = getStringProperty(JAXB_HANDLER_CLASS_PROPERTY);
        try
        {
            Class cls = clr.classForName(jaxbHandlerClassName);
            jaxbHandler = (JAXBHandler) ClassUtils.newInstance(cls, new Class[] {MetaDataManager.class}, new Object[]{ctx.getMetaDataManager()});
        }
        catch (ClassNotResolvedException cnre)
        {
            NucleusLogger.DATASTORE.error("Could not find jaxb handler class " + jaxbHandlerClassName, cnre);
            throw new NucleusUserException("The specified JAXB Handler class \"" + jaxbHandlerClassName + "\" was not found!").setFatal();
        }

        // Handler for metadata
        metadataListener = new XMLMetaDataListener();
        ctx.getMetaDataManager().registerListener(metadataListener);

        // Handler for persistence process
        persistenceHandler = new XMLPersistenceHandler(this);

        logConfiguration();
    }

    /**
     * Release of resources
     */
    public synchronized void close()
    {
        nucleusContext.getMetaDataManager().deregisterListener(metadataListener);
        super.close();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec)
    {
        if (language.equalsIgnoreCase(Query.LANGUAGE_JDOQL))
        {
            return new JDOQLQuery(this, ec);
        }
        else if (language.equalsIgnoreCase(Query.LANGUAGE_JPQL))
        {
            return new JPQLQuery(this, ec);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, java.lang.String)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, String queryString)
    {
        if (language.equalsIgnoreCase(Query.LANGUAGE_JDOQL))
        {
            return new JDOQLQuery(this, ec, queryString);
        }
        else if (language.equalsIgnoreCase(Query.LANGUAGE_JPQL))
        {
            return new JPQLQuery(this, ec, queryString);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, org.datanucleus.store.query.Query)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, Query q)
    {
        if (language.equalsIgnoreCase(Query.LANGUAGE_JDOQL))
        {
            return new JDOQLQuery(this, ec, (JDOQLQuery) q);
        }
        else if (language.equalsIgnoreCase(Query.LANGUAGE_JPQL))
        {
            return new JPQLQuery(this, ec, (JPQLQuery) q);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    public JAXBHandler getJAXBHandler()
    {
        return jaxbHandler;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getClassNameForObjectID(java.lang.Object, org.datanucleus.ClassLoaderResolver, org.datanucleus.ExecutionContext)
     */
    @Override
    public String getClassNameForObjectID(Object id, ClassLoaderResolver clr, ExecutionContext ec)
    {
        String targetClassName = null;
        if (IdentityUtils.isSingleFieldIdentity(id))
        {
            // Using SingleFieldIdentity so can assume that object is of the target class or a subclass
            targetClassName = IdentityUtils.getTargetClassNameForIdentity(id);
            String[] subclasses = getMetaDataManager().getSubclassesForClass(targetClassName, true);
            if (subclasses == null)
            {
                // No subclasses so must be the specified type
                return targetClassName;
            }

            String[] possibleNames = new String[subclasses.length+1];
            possibleNames[0] = targetClassName;
            for (int i=0;i<subclasses.length;i++)
            {
                possibleNames[i+1] = subclasses[i];
            }
            return getClassNameForIdentity(ec, possibleNames, id);
        }

        return super.getClassNameForObjectID(id, clr, ec);
    }

    /**
     * Method to return which of the possible class names for an identity corresponds to an object in the datastore. 
     * ONLY SUPPORTS SINGLE-FIELD IDENTITY.
     * @param ec execution context
     * @param possibleNames The possible class names of the object
     * @param id The identity
     * @return The class name of the object it corresponds to
     */
    public String getClassNameForIdentity(ExecutionContext ec, String[] possibleNames, Object id)
    {
        ManagedConnection mconn = connectionMgr.getConnection(ec);
        try
        {
            Document doc = (Document) mconn.getConnection();

            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            XPath xpath = XPathFactory.newInstance().newXPath();
            StringBuilder expression = null;
            for (int i=0;i<possibleNames.length;i++)
            {
                // Try to find an object of this possible class with the id
                AbstractClassMetaData acmd = getMetaDataManager().getMetaDataForClass(possibleNames[i], clr);
                expression = new StringBuilder();
                expression.append(XMLUtils.getXPathForClass(acmd));
                if (expression.length() == 0)
                {
                    if (doc.getDocumentElement() != null)
                    {
                        expression.append("/" + doc.getDocumentElement().getNodeName());
                    }
                    else
                    {
                        // No root, so can't have an object
                        continue;
                    }
                }
                expression.append('/').append(XMLUtils.getElementNameForClass(acmd));
                String[] pk = acmd.getPrimaryKeyMemberNames();
                for (int j=0; j<pk.length; j++)
                {
                    AbstractMemberMetaData pkmmd = acmd.getMetaDataForMember(pk[j]);
                    String pkElement = XMLUtils.getElementNameForMember(pkmmd, FieldRole.ROLE_FIELD);
                    Object obj = IdentityUtils.getTargetKeyForSingleFieldIdentity(id);
                    expression.append('/').append(pkElement).append("/text()='").append(obj.toString()).append("'"); 
                }

                if ((Boolean)xpath.evaluate(expression.toString(), doc, XPathConstants.BOOLEAN))
                {
                    return possibleNames[i];
                }
            }
        }
        catch (Exception e)
        {
            return null;
        }
        finally
        {
            mconn.release();
        }
        return null;
    }

    /**
     * Accessor for the supported options in string form.
     * @return The supported options
     */
    @Override
    public Collection getSupportedOptions()
    {
        Set<String> set = new HashSet<String>();
        set.add(StoreManager.OPTION_APPLICATION_ID);
        set.add(StoreManager.OPTION_TXN_ISOLATION_READ_COMMITTED);
        set.add(StoreManager.OPTION_ORM);
        return set;
    }

    /**
     * Method defining which value-strategy to use when the user specifies "native" for datastore-identity.
     * Returns "generate-id" no matter what the field is. Override if your datastore requires something else.
     * @param cmd Class requiring the strategy
     * @return Just returns "generate-id".
     */
    @Override
    public String getValueGenerationStrategyForNative(AbstractClassMetaData cmd)
    {
        return "generate-id";
    }   

    /**
     * Method defining which value-strategy to use when the user specifies "native" for a member.
     * Returns "generate-id" no matter what the field is.
     * @param mmd Member requiring the strategy
     * @return Just returns "generate-id".
     */
    @Override
    public String getValueGenerationStrategyForNative(AbstractMemberMetaData mmd)
    {
        return "generate-id";
    }   

    @Override
    protected Object getNextValueForValueGenerator(ValueGenerator generator, final ExecutionContext ec)
    {
        Object oid = null;
        synchronized (generator)
        {
            // Get the next value for this generator for this ExecutionContext
            // Note : this is synchronised since we don't want to risk handing out this generator
            // while its connectionProvider is set to that of a different ExecutionContext
            // It maybe would be good to change ValueGenerator to have a next taking the connectionProvider
            if (generator instanceof AbstractConnectedGenerator)
            {
                // datastore-based generator so set the connection provider, using connection for PM
                ValueGenerationConnectionProvider connProvider = new ValueGenerationConnectionProvider()
                {
                    ManagedConnection mconn;
                    public ManagedConnection retrieveConnection()
                    {
                        mconn = connectionMgr.getConnection(ec);
                        return mconn;
                    }
                    public void releaseConnection() 
                    {
                        mconn.release();
                        mconn = null;
                    }
                };
                ((AbstractConnectedGenerator)generator).setConnectionProvider(connProvider);
            }

            oid = generator.next();
        }
        return oid;
    }
}