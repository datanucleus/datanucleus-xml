/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.MetaDataManager;
import org.w3c.dom.Node;

/**
 * Abstract base for all JAXBHandlers so that any concrete implementation only needs to implement the getJAXBContext method.
 */
public abstract class AbstractJAXBHandler implements JAXBHandler
{
    protected MetaDataManager mmgr;

    public AbstractJAXBHandler(MetaDataManager mmgr)
    {
        this.mmgr = mmgr;
    }

    /**
     * Convenience method to return a JAXContext for the supplied classes.
     * @param classes The classes to handle
     * @param clr ClassLoader resolver
     * @return The JAXBContext
     * @throws JAXBException Thrown if an error occurs
     */
    protected abstract JAXBContext getJAXBContext(Class[] classes, ClassLoaderResolver clr) throws JAXBException;

    /* (non-Javadoc)
     * @see org.datanucleus.store.xml.JAXBHandler#marshall(java.lang.Object, org.w3c.dom.Node, org.datanucleus.ClassLoaderResolver)
     */
    @Override
    public void marshall(Object obj, Node node, ClassLoaderResolver clr) throws JAXBException
    {
        Marshaller marshaller = getJAXBContext(new Class[]{obj.getClass()}, clr).createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.marshal(obj, node);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.xml.JAXBHandler#unmarshall(java.lang.Class, org.w3c.dom.Node, org.datanucleus.ClassLoaderResolver)
     */
    @Override
    public Object unmarshall(Class cls, Node node, ClassLoaderResolver clr) throws JAXBException
    {
        return getJAXBContext(new Class[] {cls}, clr).createUnmarshaller().unmarshal(node);
    }
}