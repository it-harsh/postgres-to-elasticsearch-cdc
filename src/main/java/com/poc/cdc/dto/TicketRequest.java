package com.poc.cdc.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TicketRequest {
    private String title;
    private String description;
    private String priority;
    private String assignee;
}
