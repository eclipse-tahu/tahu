/********************************************************************************
 * Copyright (c) 2025 Cirrus Link Solutions and others
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Cirrus Link Solutions - initial implementation
 ********************************************************************************/

package org.eclipse.tahu.test;

import junit.framework.TestCase;
import org.eclipse.tahu.message.model.Topic;

public class TopicTest extends TestCase {
    public void testValidStateTopic() {
        assertTrue(Topic.isValidTopic("spBv1.0/STATE/myGroup"));
    }

    public void testValidEdgeNodeTopic() {
        assertTrue(Topic.isValidTopic("spBv1.0/myGroup/NBIRTH/myNode"));
        assertTrue(Topic.isValidTopic("spBv1.0/myGroup/NDATA/myNode"));
    }

    public void testValidDeviceTopic() {
        assertTrue(Topic.isValidTopic("spBv1.0/myGroup/DBIRTH/myNode/myDevice"));
        assertTrue(Topic.isValidTopic("spBv1.0/myGroup/DRECORD/myNode/myDevice"));
    }

    public void testInvalidNull() {
        assertFalse(Topic.isValidTopic(null));
    }

    public void testInvalidEmpty() {
        assertFalse(Topic.isValidTopic(""));
    }

    public void testInvalidPrefix() {
        assertFalse(Topic.isValidTopic("spAv1.0/myGroup/NDATA/myNode"));
    }

    public void testInvalidMessageType() {
        assertFalse(Topic.isValidTopic("spBv1.0/myGroup/UNKNOWN/myNode"));
    }

    public void testInvalidLength() {
        assertFalse(Topic.isValidTopic("spBv1.0"));
        assertFalse(Topic.isValidTopic("spBv1.0/group/NDATA"));
        assertFalse(Topic.isValidTopic("spBv1.0/Group 1/DBIRTH/Edge 1/Device 2/token1/token2"));
    }
}
