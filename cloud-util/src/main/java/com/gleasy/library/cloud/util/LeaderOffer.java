package com.gleasy.library.cloud.util;

import java.util.Comparator;

/**
 * A leader offer is a numeric id / path pair. The id is the sequential node id
 * assigned by ZooKeeper where as the path is the absolute path to the ZNode.
 */
public class LeaderOffer {

  private Integer id;
  private String nodePath;
  private String nodeName;

  public LeaderOffer() {
    // Default constructor
  }

  public LeaderOffer(Integer id, String nodePath, String hostName) {
    this.id = id;
    this.nodePath = nodePath;
    this.nodeName = hostName;
  }

  @Override
  public String toString() {
    return "{ id:" + id + " nodePath:" + nodePath + " nodeName:" + nodeName
        + " }";
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getNodePath() {
    return nodePath;
  }

  public void setNodePath(String nodePath) {
    this.nodePath = nodePath;
  }

  public String getNodeName() {
	return nodeName;
  }
	
  public void setNodeName(String nodeName) {
	this.nodeName = nodeName;
  }

  


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LeaderOffer other = (LeaderOffer) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}




/**
   * Compare two instances of {@link LeaderOffer} using only the {code}id{code}
   * member.
   */
  public static class IdComparator implements Comparator<LeaderOffer> {

    @Override
    public int compare(LeaderOffer o1, LeaderOffer o2) {
      return o1.getId().compareTo(o2.getId());
    }

  }

}
