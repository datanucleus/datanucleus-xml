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

import javax.xml.bind.annotation.XmlIDREF;

import org.datanucleus.metadata.AbstractMemberMetaData;

public class XmlIDREFHandler implements InvocationHandler
{
    @SuppressWarnings("unused")
    private AbstractMemberMetaData ammd;

    public XmlIDREFHandler(AbstractMemberMetaData ammd)
    {
        this.ammd = ammd;
    }

    public static Annotation newProxy(AbstractMemberMetaData ammd)
    {
        return (Annotation) Proxy.newProxyInstance(AbstractMemberMetaData.class.getClassLoader(),
            new Class[]{XmlIDREF.class}, new XmlIDREFHandler(ammd));
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
        String name = method.getName();
        if (name.equals("annotationType"))
        {
            return XmlIDREF.class;
        }
        return null;
    }
}
