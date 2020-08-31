package com.sap.iot.azure.ref.integration.commons.cache.redis;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TestPojo {
    private String name;
    private Integer number;
}
