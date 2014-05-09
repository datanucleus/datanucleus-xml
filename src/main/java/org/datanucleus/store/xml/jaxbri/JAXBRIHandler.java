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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.xml.AbstractJAXBHandler;

import com.sun.xml.bind.api.JAXBRIContext;

/**
 * JAXBHandler using the JAXB reference implementation.
 * See https://github.com/gf-metro/jaxb/tree/master/jaxb-ri
 */
public class JAXBRIHandler extends AbstractJAXBHandler
{
    public JAXBRIHandler(MetaDataManager mmgr)
    {
        super(mmgr);
    }

    protected JAXBContext getJAXBContext(Class[] classes, ClassLoaderResolver clr)
    throws JAXBException
    {
        Map<String, Object> jaxbConfig = new HashMap<String, Object>();
        //jaxbConfig.put(JAXBRIContext.DEFAULT_NAMESPACE_REMAP, "DefaultNamespace");
        jaxbConfig.put(JAXBRIContext.ANNOTATION_READER, new JAXBRIAnnotationReader(mmgr, clr));

        return JAXBContext.newInstance(classes, jaxbConfig);
    }
}