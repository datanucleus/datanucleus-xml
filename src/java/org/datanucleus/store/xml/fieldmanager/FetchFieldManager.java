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

import java.util.Collection;

import javax.xml.bind.JAXBException;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.xml.XMLStoreManager;
import org.datanucleus.store.xml.XMLUtils;
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
    ObjectProvider op;

    /** Unmarshalled object. */
    Object value;

    /** Document to use for reading the object. */
    Document doc;

    /** Node representing the object having its fields fetched. */
    Node node;

    public FetchFieldManager(ObjectProvider op, Document doc)
    {
        this.op = op;
        this.doc = doc;

        ExecutionContext ec = op.getExecutionContext();
        node = XMLUtils.findNode(doc, op);
        try
        {
            value = ((XMLStoreManager)ec.getStoreManager()).getJAXBHandler().unmarshall(op.getObject().getClass(), node, op.getExecutionContext().getClassLoaderResolver());
        }
        catch (JAXBException e)
        {
            NucleusLogger.DATASTORE_RETRIEVE.warn("Exception unmarshalling XML", e);
        }
    }

    public String fetchStringField(int fieldNumber)
    {
        op.copyFieldsFromObject(value, new int[]{fieldNumber});
        return (String) op.provideField(fieldNumber);
    }

    public Object fetchObjectField(int fieldNumber)
    {
        ExecutionContext ec = op.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType == RelationType.NONE)
        {
            // Object-based field (non-relation)
            op.copyFieldsFromObject(value, new int[]{fieldNumber});
            return op.provideField(fieldNumber);
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
                final AbstractClassMetaData cmd = op.getExecutionContext().getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                final NodeList nList = ((Element) node).getElementsByTagName(mmd.getName());
                if (nList.getLength() == 1)
                {
                    Object id = IdentityUtils.getNewApplicationIdentityObjectId(
                        clr.classForName(cmd.getFullClassName(), true), nList.item(0).getFirstChild().getNodeValue());
                    Object related = ec.findObject(id, true, true, null);
                    if (relationType == RelationType.ONE_TO_ONE_BI)
                    {
                        // Set other side of relation to avoid reloading
                        ObjectProvider relatedSM = ec.findObjectProvider(related);
                        AbstractMemberMetaData relatedMmd = mmd.getRelatedMemberMetaDataForObject(clr, op.getObject(), related);
                        relatedSM.replaceField(relatedMmd.getAbsoluteFieldNumber(), op.getObject());
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
                    AbstractClassMetaData cmd2 = op.getExecutionContext().getMetaDataManager().getMetaDataForClass(mmd.getCollection().getElementType(), clr);

                    // Get value being unmarshalled
                    op.copyFieldsFromObject(value, new int[]{fieldNumber});
                    Collection collection = (Collection) op.provideField(fieldNumber);

                    // Make sure we get the right type of element (allow for inheritance)
                    NodeList nLists = ((Element) node).getElementsByTagName(
                        XMLUtils.getElementNameForMember(mmd, FieldRole.ROLE_COLLECTION_ELEMENT));
                    for (int i = 0; i < nLists.getLength(); i++)
                    {
                        final String nodeValue = nLists.item(i).getFirstChild().getNodeValue();
                        if (nodeValue != null && nodeValue.trim().length() > 0)
                        {
                            final AbstractClassMetaData elementCmd = XMLUtils.findMetaDataForNode(doc, cmd2,
                                ec.getMetaDataManager(), nodeValue, clr);
                            if (elementCmd == null && cmd2 != null)
                            {
                                throw new NucleusException("Unable to find object of type " + cmd2.getFullClassName() + " with id=" + nodeValue);
                            }

                            Object id = IdentityUtils.getNewApplicationIdentityObjectId(
                                clr.classForName(elementCmd.getFullClassName(), true), nodeValue);
                            Object related = ec.findObject(id, true, true, null);
                            if (relationType == RelationType.ONE_TO_MANY_BI)
                            {
                                // Set other side of relation to avoid reloading
                                ObjectProvider relatedSM = ec.findObjectProvider(related);
                                AbstractMemberMetaData relatedMmd = relatedSM.getClassMetaData().getMetaDataForMember(mmd.getMappedBy());
                                relatedSM.replaceField(relatedMmd.getAbsoluteFieldNumber(), op.getObject());
                            }
                            collection.add(related);
                        }
                    }

                    return op.wrapSCOField(fieldNumber, collection, false, false, true);
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
        op.copyFieldsFromObject(value, new int[]{fieldNumber});
        return (Boolean) op.provideField(fieldNumber);
    }

    public byte fetchByteField(int fieldNumber)
    {
        op.copyFieldsFromObject(value, new int[]{fieldNumber});
        return (Byte) op.provideField(fieldNumber);
    }

    public char fetchCharField(int fieldNumber)
    {
        op.copyFieldsFromObject(value, new int[]{fieldNumber});
        return (Character) op.provideField(fieldNumber);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        op.copyFieldsFromObject(value, new int[]{fieldNumber});
        return (Double) op.provideField(fieldNumber);
    }

    public float fetchFloatField(int fieldNumber)
    {
        op.copyFieldsFromObject(value, new int[]{fieldNumber});
        return (Float) op.provideField(fieldNumber);
    }

    public int fetchIntField(int fieldNumber)
    {
        op.copyFieldsFromObject(value, new int[]{fieldNumber});
        return (Integer) op.provideField(fieldNumber);
    }

    public long fetchLongField(int fieldNumber)
    {
        op.copyFieldsFromObject(value, new int[]{fieldNumber});
        return (Long) op.provideField(fieldNumber);
    }

    public short fetchShortField(int fieldNumber)
    {
        op.copyFieldsFromObject(value, new int[]{fieldNumber});
        return (Short) op.provideField(fieldNumber);
    }
}