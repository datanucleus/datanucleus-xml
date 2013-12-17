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
package org.datanucleus.store.xml.jaxbri;

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.xml.JAXBHandler;
import org.w3c.dom.Node;

import com.sun.xml.bind.api.JAXBRIContext;

/**
 * JAXBHandler using the JAXB reference implementation.
 */
public class JAXBRIHandler implements JAXBHandler
{
    /* (non-Javadoc)
     * @see org.datanucleus.store.xml.JAXBHandler#marshall(java.lang.Object, org.w3c.dom.Node, org.datanucleus.metadata.MetaDataManager, org.datanucleus.ClassLoaderResolver)
     */
    @Override
    public void marshall(Object obj, Node node, MetaDataManager mmgr, ClassLoaderResolver clr) throws JAXBException
    {
        Map<String, Object> jaxbConfig = new HashMap<String, Object>();
        //jaxbConfig.put(JAXBRIContext.DEFAULT_NAMESPACE_REMAP, "DefaultNamespace");
        jaxbConfig.put(JAXBRIContext.ANNOTATION_READER, new JAXBRIAnnotationReader(mmgr, clr));

        JAXBContext jaxbContext = JAXBContext.newInstance(new Class[]{obj.getClass()}, jaxbConfig);
        Marshaller marshaller = jaxbContext.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.marshal(obj, node);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.xml.JAXBHandler#unmarshall(java.lang.Class, org.w3c.dom.Node, org.datanucleus.metadata.MetaDataManager, org.datanucleus.ClassLoaderResolver)
     */
    @Override
    public Object unmarshall(Class cls, Node node, MetaDataManager mmgr, ClassLoaderResolver clr) throws JAXBException
    {
        Map<String, Object> jaxbConfig = new HashMap<String, Object>();
        //jaxbConfig.put(JAXBRIContext.DEFAULT_NAMESPACE_REMAP, "DefaultNamespace");
        jaxbConfig.put(JAXBRIContext.ANNOTATION_READER, new JAXBRIAnnotationReader(mmgr, clr));

        JAXBContext jaxbContext = JAXBContext.newInstance(new Class[]{cls}, jaxbConfig);
        return jaxbContext.createUnmarshaller().unmarshal(node);
    }
}