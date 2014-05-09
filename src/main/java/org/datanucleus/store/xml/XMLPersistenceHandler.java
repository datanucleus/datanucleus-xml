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

Contributors:
2009 Andy Jefferson - rework of insert/update/fetch/locate
    ...
**********************************************************************/
package org.datanucleus.store.xml;

import java.util.StringTokenizer;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.PersistFieldManager;
import org.datanucleus.store.xml.fieldmanager.FetchFieldManager;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Persistence handler for persisting to XML datastores.
 */
public class XMLPersistenceHandler extends AbstractPersistenceHandler
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_XML = Localiser.getInstance(
        "org.datanucleus.store.xml.Localisation", XMLStoreManager.class.getClassLoader());

    private XPath xpath = XPathFactory.newInstance().newXPath();

    /**
     * Constructor.
     * @param storeMgr Manager for the datastore
     */
    public XMLPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    /**
     * Method to close the handler and release any resources.
     */
    public void close()
    {
    }

    /**
     * Insert the object managed by the passed ObjectProvider into the XML datastore.
     * @param op ObjectProvider
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    public void insertObject(final ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        if (op.getClassMetaData().getIdentityType() == IdentityType.APPLICATION)
        {
            // Check existence of the object since XML doesn't enforce application identity
            try
            {
                locateObject(op);
                throw new NucleusUserException(LOCALISER_XML.msg("XML.Insert.ObjectWithIdAlreadyExists",
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }
            catch (NucleusObjectNotFoundException onfe)
            {
                // Do nothing since object with this id doesn't exist
            }
        }

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            Document doc = (Document) mconn.getConnection();

            AbstractClassMetaData acmd = op.getClassMetaData();
            long startTime = 0;
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                startTime = System.currentTimeMillis();
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_XML.msg("XML.Insert.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            // Get the XPath for the objects of this class
            Node classnode = getNodeForClass(doc, acmd);

            // Marshall the object
            ((XMLStoreManager)storeMgr).getJAXBHandler().marshall(op.getObject(), classnode, op.getExecutionContext().getClassLoaderResolver());
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_XML.msg("XML.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }

            if (acmd.isVersioned())
            {
                // TODO versioned object so set its version
            }

            // Perform any reachability
            int[] fieldNumbers = op.getClassMetaData().getAllMemberPositions();
            op.provideFields(fieldNumbers, new PersistFieldManager(op, true));

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.info(LOCALISER_XML.msg("XML.Insert.ObjectPersisted",
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }
        }
        catch (Exception e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Updates the specified fields of the object managed by the passed ObjectProvider in the XML datastore.
     * @param op ObjectProvider
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     * @throws NucleusOptimisticException thrown if version checking fails
     */
    public void updateObject(final ObjectProvider op, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        AbstractClassMetaData acmd = op.getClassMetaData();
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            Document doc = (Document) mconn.getConnection();
            long startTime = 0;
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                startTime = System.currentTimeMillis();

                // Debug information about what fields we are updating
                StringBuffer str = new StringBuffer();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        str.append(",");
                    }
                    str.append(acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_XML.msg("XML.Update.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId(), str));
            }

            if (acmd.isVersioned())
            {
                // versioned object so update its version
                // TODO Implement version checks
            }

            // Get the XPath for the objects of this class
            Node classnode = getNodeForClass(doc, acmd);

            // Make sure we have all fields loaded that need to be (removing the node will lose them temporarily)
            op.loadUnloadedFields();

            // Remove old node and replace with new
            Node node = XMLUtils.findNode(doc, op);
            node.getParentNode().removeChild(node);
            ((XMLStoreManager)storeMgr).getJAXBHandler().marshall(op.getObject(), classnode, op.getExecutionContext().getClassLoaderResolver());

            // Handle reachability so any reachable objects in the updated fields are also persisted
            op.provideFields(fieldNumbers, new PersistFieldManager(op, false));

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementUpdateCount();
                ec.getStatistics().incrementNumWrites();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_XML.msg("XML.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
        }
        catch (Exception e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Deletes the object managed by the passed ObjectProvider from the XML datastore.
     * @param op ObjectProvider
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     * @throws NucleusOptimisticException thrown if version checking fails on an optimistic transaction 
     *     for this object
     */
    public void deleteObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            Document doc = (Document) mconn.getConnection();
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_XML.msg("XML.Delete.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            AbstractClassMetaData acmd = op.getClassMetaData();
            if (acmd.isVersioned())
            {
                // TODO Implement version checks
            }

            Node node = XMLUtils.findNode(doc, op);
            node.getParentNode().removeChild(node);

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_XML.msg("XML.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }
        }
        catch (Exception e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method to retrieve the specified fields of the object managed by the ObjectProvider.
     * @param op ObjectProvider
     * @param fieldNumbers Absolute field numbers to retrieve
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    public void fetchObject(final ObjectProvider op, int[] fieldNumbers)
    {
        AbstractClassMetaData cmd = op.getClassMetaData();
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                StringBuffer fieldsString = new StringBuffer();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldsString.append(",");
                    }
                    fieldsString.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_XML.msg("XML.Fetch.Start",
                    op.getObjectAsPrintable(), op.getInternalObjectId(), fieldsString));
            }

            Document doc = (Document) mconn.getConnection();

            // Find the object from XML, and populate the required fields
            op.replaceFields(fieldNumbers, new FetchFieldManager(op, doc));

            if (cmd.isVersioned())
            {
                // TODO Retrieve version ?
            }

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_XML.msg("XML.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumReads();
                ec.getStatistics().incrementFetchCount();
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Accessor for the object with the specified identity (if present).
     * Since we don't manage the memory instantiation of objects this just returns null.
     * @param ec execution context
     * @param id Identity of the object
     * @return The object
     */
    public Object findObject(ExecutionContext ec, Object id)
    {
        return null;
    }

    /**
     * Locates the object managed by the passed ObjectProvider into the XML datastore.
     * @param op ObjectProvider
     * @throws NucleusDataStoreException if an error occurs in locating the object
     */
    public void locateObject(ObjectProvider op)
    {
        AbstractClassMetaData acmd = op.getClassMetaData();
        if (acmd.getIdentityType() == IdentityType.DATASTORE)
        {
            throw new NucleusException(LOCALISER_XML.msg("XML.DatastoreID"));
        }
        else if (acmd.getIdentityType() == IdentityType.NONDURABLE)
        {
            throw new NucleusException("Nondurable not supported");
        }
        else
        {
            ExecutionContext ec = op.getExecutionContext();
            boolean isStored = false;

            // Get any node with these key values from XML
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_XML.msg("XML.Locate.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            ManagedConnection mconn = storeMgr.getConnection(ec);
            StringBuilder expression = null;
            try
            {
                Document doc = (Document) mconn.getConnection();

                // Get the XPath for the objects of this class
                expression = new StringBuilder(XMLUtils.getXPathForClass(acmd));
                if (expression.length() == 0)
                {
                    if (doc.getDocumentElement() != null)
                    {
                        expression.append("/").append(doc.getDocumentElement().getNodeName());
                    }
                    else
                    {
                        // No root, so can't have an object
                        throw new NucleusObjectNotFoundException(LOCALISER_XML.msg("XML.Object.NotFound", 
                            op.getObjectAsPrintable(), op.getInternalObjectId(), expression.toString()));
                    }
                }
                expression.append("/").append(XMLUtils.getElementNameForClass(acmd));
                String[] pk = acmd.getPrimaryKeyMemberNames();
                for (int i=0; i<pk.length; i++)
                {
                    AbstractMemberMetaData pkmmd = acmd.getMetaDataForMember(pk[i]);
                    String pkElement = XMLUtils.getElementNameForMember(pkmmd, FieldRole.ROLE_FIELD);
                    Object obj = op.provideField(acmd.getPKMemberPositions()[i]);
                    expression.append("/").append(pkElement).append("/text()='").append(obj.toString()).append("'"); 
                }

                if (ec.getStatistics() != null)
                {
                    ec.getStatistics().incrementNumReads();
                }

                isStored = (Boolean) xpath.evaluate(expression.toString(), doc, XPathConstants.BOOLEAN);
            }
            catch (Exception e)
            {
                throw new NucleusObjectNotFoundException(LOCALISER_XML.msg("XML.Object.NotFound", 
                    op.getObjectAsPrintable(), op.getInternalObjectId(), expression));
            }
            finally
            {
                mconn.release();
            }

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_XML.msg("XML.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }

            if (!isStored)
            {
                throw new NucleusObjectNotFoundException(LOCALISER_XML.msg("XML.Object.NotFound", 
                    op.getObjectAsPrintable(), op.getInternalObjectId(), expression));
            }
        }
    }
    
    /**
     * Gets the node that holds elements for the class
     * @param doc the document to get the node from
     * @param acmd the type
     * @return the node that will have child elements
     * @throws XPathExpressionException
     * @throws DOMException
     */
    private Node getNodeForClass(Document doc, AbstractClassMetaData acmd) throws XPathExpressionException, DOMException
    {
        Node classnode;
        String expression = XMLUtils.getXPathForClass(acmd);
        if (expression == null)
        {
            // No XPath defined, so find current root
            if (doc.getDocumentElement() == null)
            {
                // No current root, so add a default
                doc.appendChild(doc.createElement(XMLUtils.getDefaultRootXPath()));
            }

            // Get Node for persisting Objects
            classnode = (Node) doc.getDocumentElement();
        }
        else
        {
            // Test for existence of XPath expression, and create whole hierarchy as necessary
            if (xpath.evaluate(expression, doc, XPathConstants.NODE) == null)
            {
                StringTokenizer xpathElement = new StringTokenizer(expression, "/");
                StringBuilder path = new StringBuilder();
                String currentelement = null;
                Node node = doc;
                while (xpathElement.hasMoreElements())
                {
                    currentelement = xpathElement.nextToken();
                    path.append("/").append(currentelement);
                    Node n = (Node) xpath.evaluate(path.toString(), doc, XPathConstants.NODE);
                    if (n == null)
                    {
                        node = node.appendChild(doc.createElement(currentelement));
                    }
                    else
                    {
                        node = n;
                    }
                }
            }

            // Get Node for persisting Objects
            classnode = (Node) xpath.evaluate(expression, doc, XPathConstants.NODE);
        }
        return classnode;
    }
}