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
 2008 Eric Sultan - some handling for 1-1, 1-N
 ...
 **********************************************************************/
package org.datanucleus.store.xml.fieldmanager;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;

import javax.xml.bind.JAXBException;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.enhancement.Persistable;
import org.datanucleus.enhancement.StateManager;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.xml.XMLStoreManager;
import org.datanucleus.store.xml.XMLUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * FieldManager for retrieving field values from XML results.
 */
public class FetchFieldManager extends AbstractFieldManager
{
    DNStateManager sm;

    /** Unmarshalled object. */
    Object value;

    /** Document to use for reading the object. */
    Document doc;

    /** Node representing the object having its fields fetched. */
    Node node;

    public FetchFieldManager(DNStateManager sm, Document doc)
    {
        this.sm = sm;
        this.doc = doc;

        ExecutionContext ec = sm.getExecutionContext();
        node = XMLUtils.findNode(doc, sm);
        try
        {
            value = ((XMLStoreManager)ec.getStoreManager()).getJAXBHandler().unmarshall(sm.getObject().getClass(), node, sm.getExecutionContext().getClassLoaderResolver());
        }
        catch (JAXBException e)
        {
            NucleusLogger.DATASTORE_RETRIEVE.warn("Exception unmarshalling XML", e);
        }
    }

    public String fetchStringField(int fieldNumber)
    {
        copyFieldsFromObject(sm, value, new int[]{fieldNumber});
        return (String) sm.provideField(fieldNumber);
    }

    public Object fetchObjectField(int fieldNumber)
    {
        ExecutionContext ec = sm.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType == RelationType.NONE)
        {
            // Object-based field (non-relation)
            copyFieldsFromObject(sm, value, new int[]{fieldNumber});
            return sm.provideField(fieldNumber);
        }
        else if (mmd.getEmbeddedMetaData() != null)
        {
            // TODO Implement embedded relation objects
        }
        else
        {
            if (relationType == RelationType.ONE_TO_ONE_UNI || relationType == RelationType.ONE_TO_ONE_BI ||
                relationType == RelationType.MANY_TO_ONE_BI)
            {
                final AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                final NodeList nList = ((Element) node).getElementsByTagName(mmd.getName());
                if (nList.getLength() == 1)
                {
                    Object id = ec.getNucleusContext().getIdentityManager().getApplicationId(
                        clr.classForName(cmd.getFullClassName(), true), nList.item(0).getFirstChild().getNodeValue());
                    Object related = ec.findObject(id, true, true, null);
                    if (relationType == RelationType.ONE_TO_ONE_BI)
                    {
                        // Set other side of relation to avoid reloading
                        DNStateManager relatedSM = ec.findStateManager(related);
                        AbstractMemberMetaData relatedMmd = mmd.getRelatedMemberMetaDataForObject(clr, sm.getObject(), related);
                        relatedSM.replaceField(relatedMmd.getAbsoluteFieldNumber(), sm.getObject());
                    }
                    return related;
                }
                return null;
            }
            else if (relationType == RelationType.ONE_TO_MANY_UNI || relationType == RelationType.ONE_TO_MANY_BI)
            {
                // TODO Cater for Map/array
                if (mmd.hasCollection())
                {
                    AbstractClassMetaData cmd2 = ec.getMetaDataManager().getMetaDataForClass(mmd.getCollection().getElementType(), clr);
                    if (cmd2 == null)
                    {
                        throw new NucleusUserException("Cannot find metadata for element type " + mmd.getCollection().getElementType() + " for field=" + mmd.getFullFieldName());
                    }

                    // Get value being unmarshalled
                    copyFieldsFromObject(sm, value, new int[]{fieldNumber});
                    Collection collection = (Collection) sm.provideField(fieldNumber);

                    // Make sure we get the right type of element (allow for inheritance)
                    NodeList nLists = ((Element) node).getElementsByTagName(XMLUtils.getElementNameForMember(mmd, FieldRole.ROLE_COLLECTION_ELEMENT));
                    for (int i = 0; i < nLists.getLength(); i++)
                    {
                        final String nodeValue = nLists.item(i).getFirstChild().getNodeValue();
                        if (nodeValue != null && nodeValue.trim().length() > 0)
                        {
                            final AbstractClassMetaData elementCmd = XMLUtils.findMetaDataForNode(doc, cmd2, ec.getMetaDataManager(), nodeValue, clr);
                            if (elementCmd == null)
                            {
                                throw new NucleusException("Unable to find object of type " + cmd2.getFullClassName() + " with id=" + nodeValue);
                            }

                            Object id = ec.getNucleusContext().getIdentityManager().getApplicationId(clr.classForName(elementCmd.getFullClassName(), true), nodeValue);
                            Object related = ec.findObject(id, true, true, null);
                            if (relationType == RelationType.ONE_TO_MANY_BI)
                            {
                                // Set other side of relation to avoid reloading
                                DNStateManager relatedSM = ec.findStateManager(related);
                                AbstractMemberMetaData relatedMmd = relatedSM.getClassMetaData().getMetaDataForMember(mmd.getMappedBy());
                                relatedSM.replaceField(relatedMmd.getAbsoluteFieldNumber(), sm.getObject());
                            }
                            collection.add(related);
                        }
                    }

                    return SCOUtils.wrapSCOField(sm, fieldNumber, collection, true);
                }
                else if (mmd.hasArray())
                {
                    // TODO Implement support for arrays
                }
                else if (mmd.hasMap())
                {
                    // TODO Implement support for maps
                }
            }
            else
            {
            }
        }

        return null;
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        copyFieldsFromObject(sm, value, new int[]{fieldNumber});
        return (Boolean) sm.provideField(fieldNumber);
    }

    public byte fetchByteField(int fieldNumber)
    {
        copyFieldsFromObject(sm, value, new int[]{fieldNumber});
        return (Byte) sm.provideField(fieldNumber);
    }

    public char fetchCharField(int fieldNumber)
    {
        copyFieldsFromObject(sm, value, new int[]{fieldNumber});
        return (Character) sm.provideField(fieldNumber);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        copyFieldsFromObject(sm, value, new int[]{fieldNumber});
        return (Double) sm.provideField(fieldNumber);
    }

    public float fetchFloatField(int fieldNumber)
    {
        copyFieldsFromObject(sm, value, new int[]{fieldNumber});
        return (Float) sm.provideField(fieldNumber);
    }

    public int fetchIntField(int fieldNumber)
    {
        copyFieldsFromObject(sm, value, new int[]{fieldNumber});
        return (Integer) sm.provideField(fieldNumber);
    }

    public long fetchLongField(int fieldNumber)
    {
        copyFieldsFromObject(sm, value, new int[]{fieldNumber});
        return (Long) sm.provideField(fieldNumber);
    }

    public short fetchShortField(int fieldNumber)
    {
        copyFieldsFromObject(sm, value, new int[]{fieldNumber});
        return (Short) sm.provideField(fieldNumber);
    }

    /**
     * Convenience method to update our object with the field values from the passed object.
     * Objects need to be of the same type, and the other object should not have a StateManager.
     * @param obj The object that we should copy fields from
     * @param fieldNumbers Numbers of fields to copy
     */
    public static void copyFieldsFromObject(DNStateManager sm, Object obj, int[] fieldNumbers)
    {
        if (obj == null)
        {
            return;
        }
        Persistable myPC = (Persistable) sm.getObject();
        if (!obj.getClass().getName().equals(myPC.getClass().getName()))
        {
            return;
        }
        if (!(obj instanceof Persistable))
        {
            throw new NucleusUserException("Must be Persistable");
        }
        Persistable pc = (Persistable)obj;

        // Assign the new object to this StateManager temporarily so that we can copy its fields
        replaceStateManagerForPersistable(pc, sm);
        myPC.dnCopyFields(pc, fieldNumbers);

        // Remove the StateManager from the other object
        replaceStateManagerForPersistable(pc, null);

        // Set the loaded flags now that we have copied
        sm.markFieldsAsLoaded(fieldNumbers);
    }

    /**
     * Utility to update the passed object with the passed StateManager (can be null).
     * @param pc The object to update
     * @param sm The new state manager
     */
    protected static void replaceStateManagerForPersistable(final Persistable pc, final StateManager sm)
    {
        try
        {
            // Calls to pc.dnReplaceStateManager must be run privileged
            AccessController.doPrivileged(new PrivilegedAction()
            {
                public Object run() 
                {
                    pc.dnReplaceStateManager(sm);
                    return null;
                }
            });
        }
        catch (SecurityException e)
        {
            throw new NucleusUserException(Localiser.msg("026000"), e).setFatal();
        }
    }
}