---
fontsize: 11pt
geometry: margin=3cm
---
# Ejercicios OOP - Python

1. _Consumidores: personas humanas con dinero_
    a. Cree una clase `Human` con dos atributos: nombre y edad.
    b. Cree una clase llamada `Consumer` que herede de `Human` con las siguientes
    propiedades:
        - Un atributo `wealth` que guarde la riqueza monetaria del consumidor.
        - Un método `earn(y)` que aumente la riqueza monetaria en `y`.
        - Un método `spend(c)` que reduzca la riqueza monetaria en c o levante una excepción en caso de agotar dicha riqueza.
    c. Cree una instancia de esta clase (iniciando riqueza, nombre y edad) y haga que este
    consumidor gane una suma de dinero pero luego se funda.
    d. Herencia Multiple
        i. Cree una clase `Walk` que tenga un único método que imprima el atributo
        distance caminados.
        ii. Cree un nueva clase `HumanWalker` que hereda tanto de Human como de `Walk`.

2. _Variables de clase, métodos estáticos, setters y overloading_
    a. Utilizando la clase Human definida en el ejercicio anterior:
        i. Investigue qué es y agregue una “variable de clase”, `HAS_NOSE = True`.
        Genere dos instancias de la clase y compare el valor de este atributo para
        cada caso. ¿Cómo difieren de los atributos propios de una instancia?
        ¿Debemos instanciar la clase para acceder a este atributo?
        ii. Investigue qué es y agregue una “método estático”, `add` que imprima la
        suma de dos argumentos x e y. ¿Puede este método acceder/modificar
        el estado del objeto o de la clase? ¿Podemos llamarlo sin instanciar la
        clase?
        iii. Agregue un atributos: `__surname`. ¿Cómo puede accederlo? Investigue los
        conceptos de `setter`, `getter` y `property` y escriba un método (getter),
        `get_surname`, que nos permita acceder al atributo oculto.

3. _Módulos, sys.path hacking y (manejador) de paquetes externos_
    a. Cree un directorio `outer` y dentro de este un directorio `inner`. En el primer
    directorio genere un archivo/módulo `custom_time` con una clase `Time` que
    únicamente tiene un atributo de clase `MINUTES` con algún valor.
    b. Intente importar esta clase desde un archivo, nested.py en el directorio
    inner. En caso que esto no funcione use lo módulos `os.path` o `pathlib` para
    modificar el `sys.path` de forma tal de incluir el directorio padre en la lista
    de rutas.
    c. Utilice el manejador de paquetes, `pip`, o `poetry` para instalar el módulo ex-
    terno `pendulum`. Luego utilizando métodos de esta librería obtenga la fecha
    resultante de sumar a la fecha actual los minutos indicados por la variable de
    clase `Time`.

4. _Asserts y múltiples excepciones_
    a. Cree una nueva clase `Calculator` que contenga dos atributos, `x` e `y`. Verifique
    en el constructor, mediante un assert, que estos atributos son de tipo
    numérico.
    b. Genere un método `divide` que dentro de un bloque de try/except i) asigne
    la variable `x` al producto del carácter ‘x’ por el valor entero del atributo `x`,
    ii) compute el cociente entre la nueva variable `x` y el atributo `y`, y iii) capture el
    `TypeError` resultante.
    c. Escriba un nuevo bloque (anidado) de `try/except` para manejar el error de
    tipo anterior el cual ahora computa el cociente de los atributos y captura y
    loggea un mensaje ante un posible error de división por cero.
