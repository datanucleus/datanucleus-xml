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

 Contributors:
 ...
 **********************************************************************/
package org.datanucleus.store.xml;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ElementMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.KeyMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.ValueMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Utilities for XML datastores.
 */
public class XMLUtils
{
    private static XPath xpath = XPathFactory.newInstance().newXPath();

    /**
     * Convenience method to take an object returned by XML (from a query for example), and prepare it for passing to
     * the user. Makes sure there is a StateManager connected, with all fields marked as loaded.
     * @param obj The object (from XML)
     * @param ec execution context
     * @param acmd ClassMetaData for the object
     * @return StateManager for this object
     */
    public static DNStateManager prepareXMLObjectForUse(Object obj, ExecutionContext ec, AbstractClassMetaData acmd)
    {
        if (!ec.getApiAdapter().isPersistable(obj))
        {
            return null;
        }

        DNStateManager sm = ec.findStateManager(obj);
        if (sm == null)
        {
            // Find the identity
            Object id = null;
            if (acmd.getIdentityType() == IdentityType.DATASTORE)
            {
                throw new NucleusException(Localiser.msg("XML.DatastoreID"));
            }
            id = ec.getNucleusContext().getIdentityManager().getApplicationId(obj, acmd);
            // TODO What about nondurable?

            // Object not managed so give it a StateManager before returning it
            sm = ec.getNucleusContext().getStateManagerFactory().newForPersistentClean(ec, id, obj);
            AbstractClassMetaData cmd = sm.getClassMetaData();

            // Mark as not loaded all (non-embedded) relation fields
            int[] members = cmd.getAllMemberPositions();
            for (int i = 0; i < members.length; i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(i);
                if (mmd.getRelationType(ec.getClassLoaderResolver()) != RelationType.NONE && 
                    mmd.getEmbeddedMetaData() == null)
                {
                    sm.unloadField(mmd.getName());
                }
            }
        }

        // Wrap all unwrapped SCO fields of this instance so we can pick up any changes
        sm.replaceAllLoadedSCOFieldsWithWrappers();

        return sm;
    }

    /**
     * Accessor for correct inheritance level of an object with a specified PK field value.
     * @param doc The document
     * @param acmd Metadata for the class to start from
     * @param mmgr Metadata manager
     * @param value value of PK field to search for
     * @param clr ClassLoader resolver
     * @return Metadata for the class that this is an instance of.
     */
    public static AbstractClassMetaData findMetaDataForNode(Document doc, AbstractClassMetaData acmd, MetaDataManager mmgr, String value, ClassLoaderResolver clr)
    {
        if (acmd.getIdentityType() == IdentityType.DATASTORE)
        {
            throw new NucleusException(Localiser.msg("XML.DatastoreID"));
        }
        else if (acmd.getIdentityType() == IdentityType.APPLICATION)
        {
            try
            {
                // Get the XPath for the objects of this class
                String expression = XMLUtils.getXPathForClass(acmd);
                if (expression == null)
                {
                    if (doc.getDocumentElement() != null)
                    {
                        expression = "/" + doc.getDocumentElement().getNodeName();
                    }
                    else
                    {
                        // Nothing in the document so no point searching!
                        return null;
                    }
                }
                expression += "/" + XMLUtils.getElementNameForClass(acmd);
                String[] pk = acmd.getPrimaryKeyMemberNames();
                for (int i = 0; i < pk.length; i++)
                {
                    AbstractMemberMetaData pkmmd = acmd.getMetaDataForMember(pk[i]);
                    String pkElement = XMLUtils.getElementNameForMember(pkmmd, FieldRole.ROLE_FIELD);
                    expression += "/" + pkElement + "[text()='" + value + "']";
                }
                expression += "/..";

                Node node = (Node) xpath.evaluate(expression, doc, XPathConstants.NODE);
                if (node == null)
                {
                    String[] classNames = mmgr.getSubclassesForClass(acmd.getFullClassName(), false);
                    if (classNames != null)
                    {
                        for (int i = 0; i < classNames.length; i++)
                        {
                            AbstractClassMetaData nodeCmd = mmgr.getMetaDataForClass(classNames[i], clr);
                            nodeCmd = findMetaDataForNode(doc, nodeCmd, mmgr, value, clr);
                            if (nodeCmd != null)
                            {
                                return nodeCmd;
                            }
                        }
                    }
                }
                else
                {
                    return acmd;
                }
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
        }
        else
        {
            // Nondurable identity
        }

        return null;
    }

    /**
     * Accessor for the Node with the specified identity (if present).
     * @param doc The document
     * @param sm StateManager
     * @return The object
     * @throws NucleusObjectNotFoundException if the document is null
     */
    public static Node findNode(Document doc, DNStateManager sm)
    {
        Node node = null;

        long startTime = System.currentTimeMillis();
        if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("XML.Find.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
        }

        AbstractClassMetaData acmd = sm.getClassMetaData();
        if (acmd.getIdentityType() == IdentityType.DATASTORE)
        {
            throw new NucleusException(Localiser.msg("XML.DatastoreID"));
        }
        else if (acmd.getIdentityType() == IdentityType.APPLICATION)
        {
            try
            {
                if (doc.getDocumentElement() == null)
                {
                    // Nothing in the document so no point searching!
                    throw new NucleusObjectNotFoundException();
                }
                // Get the XPath for the objects of this class
                String expression = XMLUtils.getXPathForClass(acmd);
                if (expression == null)
                {
                    expression = "/" + doc.getDocumentElement().getNodeName();
                }
                expression += "/" + XMLUtils.getElementNameForClass(acmd);
                String[] pk = acmd.getPrimaryKeyMemberNames();
                for (int i = 0; i < pk.length; i++)
                {
                    AbstractMemberMetaData pkmmd = acmd.getMetaDataForMember(pk[i]);
                    String pkElement = XMLUtils.getElementNameForMember(pkmmd, FieldRole.ROLE_FIELD);
                    Object obj = sm.provideField(acmd.getPKMemberPositions()[i]);
                    expression += "/" + pkElement + "[text()='" + obj.toString() + "']";
                }
                expression += "/..";

                node = (Node) xpath.evaluate(expression, doc, XPathConstants.NODE);
            }
            catch (NucleusObjectNotFoundException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
        }
        else
        {
            // Non-durable identity
        }

        if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("XML.ExecutionTime", (System.currentTimeMillis() - startTime)));
        }
        return node;
    }

    /**
     * Method to return the type of the XML element to use for the specified field/property.
     * Null implies no value defined
     * @param mmd Metadata for the field/property
     * @param clr ClassLoader resolver
     * @return The type
     */
    public static Class getElementTypeForMember(AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        try
        {
            if (mmd.hasExtension("type"))
            {
                return clr.classForName(mmd.getValueForExtension("type"));
            }
            else if (mmd.hasCollection())
            {
                // Collection but no "type" known so impose element type
                return clr.classForName(mmd.getCollection().getElementType());
            }
        }
        catch (ClassNotResolvedException e)
        {
            throw new RuntimeException(e.getMessage());
        }
        return null;
    }

    /**
     * Method to return the XPath where the specified class is located in the XML file. 
     * Firstly tries "xpath" extension on the class, otherwise uses the "schema" for the class, or package, 
     * or file. If no definition is found returns null.
     * @param cmd Metadata for the class
     * @return The XPath name
     */
    public static String getXPathForClass(AbstractClassMetaData cmd)
    {
        if (cmd == null)
        {
            return null;
        }

        if (cmd.hasExtension("xpath"))
        {
            return cmd.getValueForExtension("xpath");
        }
        else if (cmd.getSchema() != null)
        {
            return cmd.getSchema();
        }
        else if (cmd.getPackageMetaData().getSchema() != null)
        {
            return cmd.getPackageMetaData().getSchema();
        }
        else if (cmd.getPackageMetaData().getFileMetaData().getSchema() != null)
        {
            return cmd.getPackageMetaData().getFileMetaData().getSchema();
        }

        return null;
    }

    /**
     * Convenience method to return the default root element name to use if none specified.
     * @return The root element name
     */
    public static String getDefaultRootXPath()
    {
        return "root";
    }

    /**
     * Method to return the name of the XML element where we store the objects of the specified type. Uses "name"
     * extension on the class, otherwise the table name for the class, otherwise tries to respect inheritance strategy
     * settings, otherwise uses the lowercase form of the class name.
     * @param cmd Metadata for the class
     * @return Name of the XML element where we store objects of this type
     */
    public static String getElementNameForClass(AbstractClassMetaData cmd)
    {
        if (cmd == null)
        {
            return null;
        }

        if (cmd.hasExtension("name"))
        {
            return cmd.getValueForExtension("name");
        }
        else if (cmd.getTable() != null)
        {
            return cmd.getTable();
        }
        /*else if (cmd.getInheritanceMetaData() != null &&
            cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.NEW_TABLE)
        {
            // Use lowercase form of the name
            return cmd.getName().toLowerCase();
        }
        else if (cmd.getInheritanceMetaData() != null &&
            cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUPERCLASS_TABLE)
        {
            // Use same location as the superclass
            return getElementNameForClass(cmd.getSuperAbstractClassMetaData());
        }*/
        else
        {
            // Fallback to lowercase form of the name (JAXB default)
            return getJAXBDefaultNameForName(cmd.getName());
        }
    }

    /**
     * Method to return the name of the XML element to use for the specified field/property.
     * Tries the extension "name" for the component being named, otherwise uses the column name, 
     * otherwise falls back to the JAXB default (if a field), or a predefined default if a component of 
     * a collection/array/map.
     * @param mmd Metadata for the field/property
     * @param role Role within this field
     * @return The name
     */
    public static String getElementNameForMember(AbstractMemberMetaData mmd, FieldRole role)
    {
        if (role == FieldRole.ROLE_COLLECTION_ELEMENT && mmd.hasCollection())
        {
            // Element of a collection
            ElementMetaData elemmd = mmd.getElementMetaData();
            if (elemmd != null && elemmd.hasExtension("name"))
            {
                // Use "name" extension for element
                return mmd.getElementMetaData().getValueForExtension("name");
            }

            ColumnMetaData[] colmds = (elemmd != null ? elemmd.getColumnMetaData() : null);
            if (colmds != null && colmds.length > 0 && colmds[0].getName() != null)
            {
                // Use column name for element
                return colmds[0].getName();
            }
            // Fallback to "{fieldName}_element"
            return mmd.getName().toLowerCase() + "_element";
        }
        else if (role == FieldRole.ROLE_ARRAY_ELEMENT && mmd.hasArray())
        {
            // Element of an array
            ElementMetaData elemmd = mmd.getElementMetaData();
            if (elemmd != null && elemmd.hasExtension("name"))
            {
                // Use "name" extension for element
                return elemmd.getValueForExtension("name");
            }

            ColumnMetaData[] colmds = (elemmd != null ? elemmd.getColumnMetaData() : null);
            if (colmds != null && colmds.length > 0 && colmds[0].getName() != null)
            {
                // Use column name for element
                return colmds[0].getName();
            }
            // Fallback to "{fieldName}_element"
            return mmd.getName().toLowerCase() + "_element";
        }
        else if (role == FieldRole.ROLE_MAP_KEY && mmd.hasMap())
        {
            // Key of a map
            KeyMetaData keymd = mmd.getKeyMetaData();
            if (keymd != null && keymd.hasExtension("name"))
            {
                // Use "name" extension for element
                return keymd.getValueForExtension("name");
            }

            ColumnMetaData[] colmds = (keymd != null ? keymd.getColumnMetaData() : null);
            if (colmds != null && colmds.length > 0 && colmds[0].getName() != null)
            {
                // Use column name for key
                return colmds[0].getName();
            }
            // Fallback to "{fieldName}_key"
            return mmd.getName().toLowerCase() + "_key";
        }
        else if (role == FieldRole.ROLE_MAP_VALUE && mmd.hasMap())
        {
            // Value of a map
            ValueMetaData valuemd = mmd.getValueMetaData();
            if (valuemd != null && valuemd.hasExtension("name"))
            {
                // Use "name" extension for element
                return valuemd.getValueForExtension("name");
            }

            ColumnMetaData[] colmds = (valuemd != null ? valuemd.getColumnMetaData() : null);
            if (colmds != null && colmds.length > 0 && colmds[0].getName() != null)
            {
                // Use column name for value
                return colmds[0].getName();
            }
            // Fallback to "{fieldName}_value"
            return mmd.getName().toLowerCase() + "_value";
        }
        else
        {
            // Field as a whole
            if (mmd.hasExtension("name"))
            {
                return mmd.getValueForExtension("name");
            }
            else if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0 && 
                mmd.getColumnMetaData()[0].getName() != null)
            {
                return mmd.getColumnMetaData()[0].getName();
            }
            else
            {
                // Fallback to the name of the field (JAXB default)
                return getJAXBDefaultNameForName(mmd.getName());
            }
        }
    }

    /**
     * Convenience method to return the JAXB default for the name of a class/field. 
     * Simply changes the first character to lowercase.
     * @param name The original name
     * @return The JAXB name
     */
    public static String getJAXBDefaultNameForName(String name)
    {
        return name.substring(0, 1).toLowerCase() + name.substring(1);
    }
}