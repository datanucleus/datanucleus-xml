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
    ...
 **********************************************************************/
package org.datanucleus.store.xml.binder;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import javax.xml.bind.annotation.XmlType;

import org.datanucleus.metadata.AbstractClassMetaData;

public class XmlTypeHandler implements InvocationHandler
{
    private AbstractClassMetaData acmd;

    private XmlTypeHandler(AbstractClassMetaData acmd)
    {
        this.acmd = acmd;
    }

    public static Annotation newProxy(AbstractClassMetaData acmd)
    {
        return (Annotation) Proxy.newProxyInstance(AbstractClassMetaData.class.getClassLoader(), 
            new Class[]{XmlType.class}, new XmlTypeHandler(acmd));
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
        String name = method.getName();
        if (name.equals("getClassValue"))
        {
            name = (String) args[1];
        }

        if (name.equals("annotationType"))
        {
            return XmlType.class;
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
            return value;
        }
        else if (name.equals("propOrder"))
        {
            if (acmd.hasExtension("propOrder"))
            {
                return acmd.getValueForExtension("propOrder").split(",");
            }
            return new String[0];
        }
        else if (name.equals("factoryMethod"))
        {
            String value = "";
            if (acmd.hasExtension("factoryMethod"))
            {
                value = acmd.getValueForExtension("factoryMethod");
            }
            return value;
        }
        else if (name.equals("factoryClass"))
        {
            if (acmd.hasExtension("factoryClass"))
            {
                try
                {
                    return Class.forName(acmd.getValueForExtension("factoryClass"));
                }
                catch (ClassNotFoundException e)
                {
                    throw new RuntimeException(e.getMessage());
                }
            }
            return XmlType.DEFAULT.class;
        }
        return null;
    }
}