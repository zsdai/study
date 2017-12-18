package com.demo.service;

import java.io.Serializable;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;

/**  
**********************************************
* @ClassName: BaseService  
* @Description:
* @author 代祯山 
* @date 2017年12月18日 下午12:26:50 
* @param <T>  
**********************************************
*/ 
public interface BaseService<T> {
	/**  
	* @Title: getRepository 
	* @Description:
	* @return
	*/
	 JpaRepository<T, Serializable> getRepository();
	/**  
	* @Title: getPage 
	* @Description:
	* @param pageNumber
	* @param pageSize
	* @return
	*/
	public Page<T> getPage(int pageNumber, int pageSize,Sort sort);
}
