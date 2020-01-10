/*
 * Copyright 2019 The JIMDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.jimdb.core.model.meta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.jimdb.pb.Basepb;
import io.jimdb.common.utils.lang.ByteUtil;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.google.common.primitives.UnsignedBytes;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * range info
 *
 * @version 1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public final class RangeInfo implements Comparable, Cloneable {

  // range id
  private long id;

  // the start of range scope
  private byte[] startKey;

  // the end of range scope
  private byte[] endKey;

  // the store peer of range
  private List<Basepb.Peer> peerList;

  // the key-value pairs of range that stored node id and stored node info
  private Map<Long, Basepb.Node> nodeMap;  // nodeId : Node

  // the leader node of range
  private Basepb.Node leaderNode;
  private Basepb.RangeEpoch rangeEpoch;

  // table id
  private long tableId;

  public RangeInfo() {
  }

  public long getId() {
    return this.id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public List<Basepb.Peer> getPeerList() {
    return this.peerList;
  }

  public void setPeerList(List<Basepb.Peer> peerList) {
    this.peerList = peerList;
  }

  public Map<Long, Basepb.Node> getNodeMap() {
    return nodeMap;
  }

  public void setNodeMap(Map<Long, Basepb.Node> nodeMap) {
    this.nodeMap = nodeMap;
  }

  public Basepb.Node getLeaderNode() {
    return leaderNode;
  }

  public void setLeaderNode(Basepb.Node leaderNode) {
    this.leaderNode = leaderNode;
  }

  public Basepb.RangeEpoch getRangeEpoch() {
    return this.rangeEpoch;
  }

  public void setRangeEpoch(Basepb.RangeEpoch rangeEpoch) {
    this.rangeEpoch = rangeEpoch;
  }

  public String getLeaderAddr() {
//    return leaderNode == null ? "" : String.format("%s:%d", leaderNode.getIp(), leaderNode.getServerPort());
    return leaderNode == null ? "" : leaderNode.getIp() + ":" + leaderNode.getServerPort();
  }

  public long getTableId() {
    return this.tableId;
  }

  public void setTableId(long tableId) {
    this.tableId = tableId;
  }

  public byte[] getStartKey() {
    return startKey;
  }

  public void setStartKey(byte[] startKey) {
    this.startKey = startKey;
  }

  public byte[] getEndKey() {
    return endKey;
  }

  public void setEndKey(byte[] endKey) {
    this.endKey = endKey;
  }

  @Override
  public String toString() {
    ToStringBuilder builder = new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
            .append("id", id);
    if (leaderNode != null) {
      builder.append("leaderId", leaderNode.getId())
              .append("leaderNode", getLeaderAddr());
    }
    return builder.append("tableId", tableId)
            .append("startKey", startKey)
            .append("endKey", endKey)
            .append("conf_ver", rangeEpoch.getConfVer())
            .append("version", rangeEpoch.getVersion()).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof RangeInfo)) {
      return false;
    }
    RangeInfo rangeInfo = (RangeInfo) obj;
    return new EqualsBuilder().append(this.id, rangeInfo.getId()).append(this.tableId, rangeInfo.getTableId()).isEquals();
  }

  /**
   * 穷举所有比较情况
   * <p>
   * get的情况
   * 场景1：[   key  )
   * 场景2：[            ) key
   * 场景3：key [          )
   * <p>
   * put的情况
   * [___self____)
   * [___compared___)
   * <p>
   * [___self____)
   * [___compared___)
   * <p>
   * [___self____)
   * [___compared___)
   * <p>
   * [___self____)
   * [___compared___)
   * <p>
   * [___self____)
   * [___compared___)
   * <p>
   * [___self____)
   * [___compared___)
   * <p>
   * [___self____)
   * [___compared___)
   *
   * @param o
   * @return
   */
  @Override
  public int compareTo(Object o) {
    RangeInfo rangeInfo = (RangeInfo) o;
    byte[] startKey = rangeInfo.startKey;
    byte[] endKey = rangeInfo.endKey;

    // 处理get的情况
    if (Arrays.equals(this.startKey, this.endKey)) {
      byte[] queryKey = this.startKey;
      if (compare(queryKey, startKey) >= 0 && compare(queryKey, endKey) < 0) {
        return 0;
      }
      if (compare(queryKey, startKey) < 0) {
        return -1;
      }
      if (compare(queryKey, endKey) >= 0) {
        return 1;
      }
    } else {
      // 处理put的情况
      if (compare(this.startKey, endKey) >= 0) {
        return 1;
      }
      if (compare(this.endKey, startKey) <= 0) {
        return -1;
      }

      if (compare(this.startKey, startKey) <= 0 && compare(this.endKey, startKey) > 0) {
        return 0;
      }

      if (compare(this.startKey, startKey) >= 0 && compare(this.startKey, endKey) < 0) {
        return 0;
      }
    }

    // range之间关系场景未覆盖到，则程序有漏洞，抛出异常
    throw new RuntimeException(String.format("rang info[%s -- %s] not found in the tree",
            ByteUtil.bytes2hex01(this.startKey), ByteUtil.bytes2hex01(this.endKey)));
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + (int) (id ^ (id >>> 32));
    return 31 * result + (int) (tableId ^ (tableId >>> 32));
  }

  /**
   * 对比两个byte数组的大小
   *
   * @param a byte[]
   * @param b byte[]
   * @return a > b 返回 1, a = b 返回 0, a < b 返回 -1
   */
  private int compare(byte[] a, byte[] b) {
    return UnsignedBytes.lexicographicalComparator().compare(a, b);
  }

  public RangeInfo clone() {
    RangeInfo newRangeInfo = null;
    try {
      newRangeInfo = (RangeInfo) super.clone();
    } catch (Throwable e) {
      newRangeInfo = new RangeInfo();
    }

    newRangeInfo.startKey = this.startKey;
    newRangeInfo.endKey = this.endKey;
    newRangeInfo.setId(this.getId());
    newRangeInfo.setTableId(this.getTableId());
    newRangeInfo.setLeaderNode(this.getLeaderNode());
    newRangeInfo.setRangeEpoch(this.getRangeEpoch());
    newRangeInfo.setNodeMap(this.getNodeMap());
    newRangeInfo.setPeerList(this.getPeerList());
    return newRangeInfo;
  }
}
