@rem ================= Galaktika centralized config =================
@rem Укажите путь к каталогу с исполняемыми файлами Galaktika (без кавычек).
@rem Примеры:
@rem   C:\GalaktikaCorp\gal91\exe
@rem   \\server\share\Galaktika\gal91\exe
@rem   D:\Apps\Galaktika\gal91\exe

set "GALAKTIKA_EXE=C:\GalaktikaCorp\gal91\exe"

@rem Проверка наличия каталога:
if not exist "%GALAKTIKA_EXE%\" (
  echo [ERROR] Не найден каталог %%GALAKTIKA_EXE%%: "%GALAKTIKA_EXE%"
  exit /b 1
)
@rem ================================================================
