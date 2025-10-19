package ru.practicum.shopStopListProducer.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.shopStopListProducer.service.StopListService;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping(value = "/stop-list")
public class StopListController {
    private final StopListService stopListService;

    @PostMapping
    public String createStopListItem(@RequestBody String Item){
        return stopListService.createStopListItem(Item);
    }

    @DeleteMapping
    public String deleteStopListItem(@RequestBody String Item){
        return stopListService.deleteStopListItem(Item);
    }

    @GetMapping
    public List<String> getStopListItems(){
        return stopListService.getStopListItems();
    }

    @PostMapping("/run")
    public void runShopStopListProducer() {
        stopListService.runShopStopListProducer();
    }
}
