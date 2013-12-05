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

import javax.xml.bind.annotation.XmlAttribute;

import org.datanucleus.metadata.AbstractMemberMetaData;

public class XmlAttributeHandler implements InvocationHandler
{
    private AbstractMemberMetaData ammd;

    public XmlAttributeHandler(AbstractMemberMetaData ammd)
    {
        this.ammd = ammd;
    }

    public static Annotation newProxy(AbstractMemberMetaData ammd)
    {
        return (Annotation) Proxy.newProxyInstance(AbstractMemberMetaData.class.getClassLoader(), 
            new Class[]{XmlAttribute.class}, new XmlAttributeHandler(ammd));
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
        String name = method.getName();

        if (name.equals("annotationType"))
        {
            return XmlAttribute.class;
        }
        else if (name.equals("namespace"))
        {
            String value = "##default";
            if (ammd.hasExtension("namespace"))
            {
                value = ammd.getValueForExtension("namespace");
            }
            return value;
        }
        else if (name.equals("name"))
        {
            String value = "##default";
            if (ammd.hasExtension("name"))
            {
                value = ammd.getValueForExtension("name");
            }
            return value;
        }
        else if (name.equals("required"))
        {
            boolean value = false;
            if (ammd.hasExtension("required"))
            {
                value = Boolean.valueOf(ammd.getValueForExtension("required"));
            }
            return value;
        }
        return null;
    }
}