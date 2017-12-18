package com.demo.web;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.data.domain.Page;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.demo.entity.User;
import com.demo.service.UserService;
import com.github.pagehelper.PageInfo;

@Controller
@RequestMapping("/user")
public class UserController {

    @Resource
    UserService userService;


    @RequestMapping("/")
    public String index() {
        return "redirect:/user/page";
    }

    /**  
    * @Title: page 
    * @author 代祯山  
    * @Description:layui+jpa 分页
    * @param model
    * @param pageSize
    * @param pageNumber
    * @return
    */
    @RequestMapping("/page")
    public String page(Model model,
    		@RequestParam(value = "pageSize", defaultValue = "10") int pageSize,
    		@RequestParam(value = "pageNumber", defaultValue = "1") int pageNumber) {
		Page<User> page = userService.getPage(pageNumber, pageSize, null);
        model.addAttribute("page", page);
        return "user/list";
    }
    
    /**  
    * @Title: pageWithMybatis 
    * @author 代祯山  
    * @Description:layui+mybatis 分页
    * @param model
    * @param pageSize
    * @param pageNumber
    * @return
    */
    @RequestMapping("/page_mybatis")
    public String pageWithMybatis(Model model,
    		@RequestParam(value = "pageSize", defaultValue = "10") int pageSize,
    		@RequestParam(value = "pageNumber", defaultValue = "1") int pageNumber) {
		PageInfo<User> page = userService.getPageWithMybatis(pageNumber, pageSize);
        model.addAttribute("page", page);
        return "user/list";
    }
    
    @RequestMapping("/list")
    public String list(Model model) {
        List<User> users=userService.getUserList();
        model.addAttribute("users", users);
        return "user/list";
    }

    @RequestMapping("/toAdd")
    public String toAdd() {
        return "user/userAdd";
    }

    @RequestMapping("/add")
    public String add(User user) {
        userService.save(user);
        return "redirect:/user/page";
    }

    @RequestMapping("/toEdit")
    public String toEdit(Model model,Long id) {
        User user=userService.findUserById(id);
        model.addAttribute("user", user);
        return "user/userEdit";
    }

    @RequestMapping("/edit")
    public String edit(User user) {
        userService.edit(user);
        return "redirect:/user/page";
    }


    @RequestMapping("/delete")
    public String delete(Long id) {
        userService.delete(id);
        return "redirect:/user/page";
    }
}
