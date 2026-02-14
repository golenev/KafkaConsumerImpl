package com.validator.e2e.tests

import io.qameta.allure.Allure

fun <T> step(description: String, block: () -> T): T = Allure.step(description, block)
