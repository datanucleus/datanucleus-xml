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
import org.datanucleus.NucleusContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataListener;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationConnectionProvider;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.store.xml.jaxbri.JAXBRIHandler;
import org.datanucleus.util.ClassUtils;
import org.w3c.dom.Document;

/**
 * Manager for XML datastores.
 * Relies on JAXB API and an implementation of JAXB to be present.
 */
public class XMLStoreManager extends AbstractStoreManager
{
    JAXBHandler jaxbHandler;
    MetaDataListener metadataListener;

    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param ctx Context
     * @param props Properties for this store manager
     */
    public XMLStoreManager(ClassLoaderResolver clr, NucleusContext ctx, Map<String, Object> props)
    {
        super("xml", clr, ctx, props);

        // TODO Support other JAXB implementations
        // Check if JAXB API/RI JARs are in CLASSPATH
        ClassUtils.assertClassForJarExistsInClasspath(clr, "javax.xml.bind.JAXBContext", "jaxb-api.jar");
        ClassUtils.assertClassForJarExistsInClasspath(clr, "com.sun.xml.bind.api.JAXBRIContext", "jaxb-impl.jar");
        jaxbHandler = new JAXBRIHandler();

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
    public void close()
    {
        nucleusContext.getMetaDataManager().deregisterListener(metadataListener);
        super.close();
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
        if (getApiAdapter().isSingleFieldIdentity(id))
        {
            // Using SingleFieldIdentity so can assume that object is of the target class or a subclass
            targetClassName = getApiAdapter().getTargetClassNameForSingleFieldIdentity(id);
            String[] subclasses = getMetaDataManager().getSubclassesForClass(targetClassName, true);
            if (subclasses == null)
            {
                // No subclasses so must be the specified type
                return targetClassName;
            }

            String[] possibleNames = new String[(subclasses != null) ? (subclasses.length+1) : 1];
            possibleNames[0] = targetClassName;
            if (subclasses != null)
            {
                for (int i=0;i<subclasses.length;i++)
                {
                    possibleNames[i+1] = subclasses[i];
                }
            }
            return getClassNameForIdentity(ec, possibleNames, id);
        }

        return super.getClassNameForObjectID(id, clr, ec);
    }

    /**
     * Method to return which of the possible class names for an identity corresponds to an object
     * in the datastore. 
     * ONLY SUPPORTS SINGLE-FIELD IDENTITY.
     * @param ec execution context
     * @param possibleNames The possible class names of the object
     * @param id The identity
     * @return The class name of the object it corresponds to
     */
    public String getClassNameForIdentity(ExecutionContext ec, String[] possibleNames, Object id)
    {
        ManagedConnection mconn = getConnection(ec);
        String expression = null;
        try
        {
            Document doc = (Document) mconn.getConnection();

            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            XPath xpath = XPathFactory.newInstance().newXPath();
            for (int i=0;i<possibleNames.length;i++)
            {
                // Try to find an object of this possible class with the id
                AbstractClassMetaData acmd = getMetaDataManager().getMetaDataForClass(possibleNames[i], clr);
                expression = XMLUtils.getXPathForClass(acmd);
                if (expression == null)
                {
                    if (doc.getDocumentElement() != null)
                    {
                        expression = "/" + doc.getDocumentElement().getNodeName();
                    }
                    else
                    {
                        // No root, so can't have an object
                        continue;
                    }
                }
                expression += "/" + XMLUtils.getElementNameForClass(acmd);
                String[] pk = acmd.getPrimaryKeyMemberNames();
                for (int j=0; j<pk.length; j++)
                {
                    AbstractMemberMetaData pkmmd = acmd.getMetaDataForMember(pk[j]);
                    String pkElement = XMLUtils.getElementNameForMember(pkmmd, FieldRole.ROLE_FIELD);
                    Object obj = ec.getApiAdapter().getTargetKeyForSingleFieldIdentity(id);
                    expression += "/" + pkElement + "/text()='" + obj.toString() + "'"; 
                }

                if ((Boolean)xpath.evaluate(expression, doc, XPathConstants.BOOLEAN))
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
    public Collection getSupportedOptions()
    {
        Set<String> set = new HashSet<String>();
        set.add("DatastoreIdentity");        
        set.add("ApplicationIdentity");
        set.add("TransactionIsolationLevel.read-committed");
        set.add("ORM");
        return set;
    }
    
    /**
     * Method defining which value-strategy to use when the user specifies "native".
     * Returns "generate-id" no matter what the field is. Override if your datastore requires something else.
     * @param cmd Class requiring the strategy
     * @param absFieldNumber Field of the class
     * @return Just returns "generate-id".
     */
    protected String getStrategyForNative(AbstractClassMetaData cmd, int absFieldNumber)
    {
        return "generate-id";
    }   
    
    /**
     * Accessor for the next value from the specified generator.
     * This implementation simply returns generator.next(). Any case where the generator requires
     * datastore connections should override this method.
     * @param generator The generator
     * @param ec ExecutionContext
     * @return The next value.
     */
    protected Object getStrategyValueForGenerator(ValueGenerator generator, final ExecutionContext ec)
    {
        Object oid = null;
        synchronized (generator)
        {
            // Get the next value for this generator for this ExecutionContext
            // Note : this is synchronised since we don't want to risk handing out this generator
            // while its connectionProvider is set to that of a different ExecutionContext
            // It maybe would be good to change ValueGenerator to have a next taking the connectionProvider
            if (generator instanceof AbstractDatastoreGenerator)
            {
                // datastore-based generator so set the connection provider, using connection for PM
                ValueGenerationConnectionProvider connProvider = new ValueGenerationConnectionProvider()
                {
                    ManagedConnection mconn;
                    public ManagedConnection retrieveConnection()
                    {
                        mconn = getConnection(ec);
                        return mconn;
                    }
                    public void releaseConnection() 
                    {
                        mconn.release();
                        mconn = null;
                    }
                };
                ((AbstractDatastoreGenerator)generator).setConnectionProvider(connProvider);
            }

            oid = generator.next();
        }
        return oid;
    }
}