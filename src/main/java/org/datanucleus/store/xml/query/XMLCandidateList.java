/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.xml.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.xml.bind.JAXBException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.AbstractCandidateLazyLoadList;
import org.datanucleus.store.xml.XMLStoreManager;
import org.datanucleus.store.xml.XMLUtils;
import org.datanucleus.util.NucleusLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Wrapper for a List of candidate instances from XML. Loads the instances from the XML file lazily.
 */
public class XMLCandidateList extends AbstractCandidateLazyLoadList
{
    ManagedConnection mconn;

    boolean ignoreCache;

    /** Number of objects per class, in same order as class meta-data. */
    List<Integer> numberInstancesPerClass = null;

    /**
     * Constructor for the lazy loaded ODF candidate list.
     * @param cls The candidate class
     * @param subclasses Whether to include subclasses
     * @param ec execution context
     * @param cacheType Type of caching
     * @param mconn Connection to the datastore
     * @param ignoreCache Whether to ignore the cache on object retrieval
     */
    public XMLCandidateList(Class cls, boolean subclasses, ExecutionContext ec, String cacheType,
            ManagedConnection mconn, boolean ignoreCache)
    {
        super(cls, subclasses, ec, cacheType);
        this.mconn = mconn;
        this.ignoreCache = ignoreCache;

        // Count the instances per class by scanning the associated worksheets
        Document doc = (Document) mconn.getConnection();
        XPath xpath = XPathFactory.newInstance().newXPath();
        numberInstancesPerClass = new ArrayList<Integer>();
        Iterator<AbstractClassMetaData> cmdIter = cmds.iterator();
        while (cmdIter.hasNext())
        {
            AbstractClassMetaData cmd = cmdIter.next();

            Element el = null;
            String expression = XMLUtils.getXPathForClass(cmd);
            if (expression == null)
            {
                el = doc.getDocumentElement();
            }
            else
            {
                try
                {
                    el = (Element) xpath.evaluate(expression, doc, XPathConstants.NODE);
                }
                catch (XPathExpressionException e)
                {
                    NucleusLogger.DATASTORE_RETRIEVE.warn("Exception evaluating XPath " + expression, e);
                }
            }

            int size = 0;
            if (el != null)
            {
                String classElementName = XMLUtils.getElementNameForClass(cmd);
                for (int i = 0; i < el.getChildNodes().getLength(); i++)
                {
                    if (el.getChildNodes().item(i) instanceof Element)
                    {
                        Element elem = (Element)el.getChildNodes().item(i);
                        if (classElementName.equals(elem.getNodeName()))
                        {
                            // Valid element for an object of this type
                            size++;
                        }
                    }
                }                
            }
            numberInstancesPerClass.add(size);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractLazyLoadList#getSize()
     */
    @Override
    protected int getSize()
    {
        int size = 0;

        Iterator<Integer> numberIter = numberInstancesPerClass.iterator();
        while (numberIter.hasNext())
        {
            size += numberIter.next();
        }

        return size;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractLazyLoadList#retrieveObjectForIndex(int)
     */
    @Override
    protected Object retrieveObjectForIndex(int index)
    {
        if (index < 0 || index >= getSize())
        {
            throw new NoSuchElementException();
        }

        Document doc = (Document) mconn.getConnection();
        XPath xpath = XPathFactory.newInstance().newXPath();
        Iterator<AbstractClassMetaData> cmdIter = cmds.iterator();
        Iterator<Integer> numIter = numberInstancesPerClass.iterator();
        int first = 0;
        int last = -1;
        while (cmdIter.hasNext())
        {
            final AbstractClassMetaData cmd = cmdIter.next();
            int number = numIter.next();
            last = first+number;

            if (index >= first && index < last)
            {
                // Object is of this candidate type, so find the object
                ClassLoaderResolver clr = ec.getClassLoaderResolver();
                int current = first;
                Element el = null;
                String expression = XMLUtils.getXPathForClass(cmd);
                if (expression == null)
                {
                    el = doc.getDocumentElement();
                }
                else
                {
                    try
                    {
                        el = (Element) xpath.evaluate(expression, doc, XPathConstants.NODE);
                    }
                    catch (XPathExpressionException e)
                    {
                        NucleusLogger.DATASTORE_RETRIEVE.warn("Exception evaluating XPath " + expression, e);
                    }
                }

                if (el != null)
                {
                    // Get name of the element that objects of this type are stored under
                    Class cls = clr.classForName(cmd.getFullClassName());
                    String classElementName = XMLUtils.getElementNameForClass(cmd);
                    for (int i = 0; i < el.getChildNodes().getLength(); i++)
                    {
                        if (el.getChildNodes().item(i) instanceof Element)
                        {
                            Element elem = (Element)el.getChildNodes().item(i);
                            if (classElementName.equals(elem.getNodeName()))
                            {
                                // Valid element for an object of this type so unmarshall it and add it
                                if (current == index)
                                {
                                    try
                                    {
                                        Object obj = ((XMLStoreManager)ec.getStoreManager()).getJAXBHandler().unmarshall(cls, el.getChildNodes().item(i), clr);
                                        XMLUtils.prepareXMLObjectForUse(obj, ec, cmd);
                                        return obj;
                                    }
                                    catch (JAXBException jaxbe)
                                    {
                                        throw new NucleusUserException("Error in extracting object from XML", jaxbe);
                                    }
                                }

                                current++;
                            }
                        }
                    }
                }
            }
            else
            {
                first += number;
            }
        }
        return null;
    }
}