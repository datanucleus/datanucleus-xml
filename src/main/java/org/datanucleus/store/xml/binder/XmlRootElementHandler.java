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
2009 Eric Sultan - fix to use table name
    ...
**********************************************************************/
package org.datanucleus.store.xml.binder;

import org.datanucleus.metadata.AbstractClassMetaData;

import javax.xml.bind.annotation.XmlRootElement;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class XmlRootElementHandler implements InvocationHandler
{
    private AbstractClassMetaData acmd;

    private XmlRootElementHandler(AbstractClassMetaData acmd)
    {
        this.acmd = acmd;
    }

    public static Annotation newProxy(AbstractClassMetaData acmd)
    {
        return (Annotation) Proxy.newProxyInstance(AbstractClassMetaData.class.getClassLoader(), new Class[]{XmlRootElement.class},
            new XmlRootElementHandler(acmd));
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
        String name = method.getName();
        if (name.equals("annotationType"))
        {
            return XmlRootElement.class;
        }
        else if (name.equals("namespace"))
        {
            String value = "##default";
            if (acmd.hasExtension("namespace"))
            {
                value = acmd.getValueForExtension("namespace");
            }
            return value;
        }
        else if (name.equals("name"))
        {
            String value = "##default";
            if (acmd.hasExtension("name"))
            {
                value = acmd.getValueForExtension("name");
            }
            else if (acmd.getTable() != null && acmd.getTable().trim().length() > 0)
            {
                value = acmd.getTable();
            }
            return value;
        }

        return null;
    }
}