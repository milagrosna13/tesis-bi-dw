




-- =========================================
-- VALIDACIONES DATA WAREHOUSE
-- Proyecto: Tesis Final
-- Autor: ...
-- Fecha: ...
-- =========================================

-- 1. Validaciones estructurales
-- 2. Validaciones de integridad referencial
-- 3. Validaciones de consistencia de datos
-- 4. Validaciones de métricas
-- 5. Validaciones temporales
-- 6. Validaciones de completitud
-- 7. Validaciones de duplicados
-- 8. Consulta de métricas de control en el Data Warehouse
-- 9. Consultas de control en el origen
-- =========================================

-- 1. Validaciones estructurales
-- Verificar que no haya nulos en las claves foráneas (FK) de la tabla de hechos
SELECT COUNT(*) AS nulos_en_FK
FROM dw.fact_ventas
WHERE id_producto IS NULL OR id_cliente IS NULL OR id_tiempo IS NULL;

-- =========================================


-- 2. Validaciones de integridad referencial
--Se verifica que todas las claves foráneas de la tabla de hechos posean correspondencia 
--en sus respectivas dimensiones, garantizando la integridad del modelo estrella.
SELECT COUNT(*) AS ventas_sin_producto
FROM dw.fact_ventas f
LEFT JOIN dw.dim_producto p ON f.id_producto = p.id_producto
WHERE p.id_producto IS NULL;

SELECT COUNT(*) AS promociones_invalidas
FROM dw.fact_ventas f
LEFT JOIN dw.dim_promocion p ON f.id_promocion = p.id_promocion
WHERE f.id_promocion IS NOT NULL
  AND p.id_promocion IS NULL;

SELECT *
FROM dw.fact_ventas
WHERE id_producto IS NULL
   OR id_cliente IS NULL
   OR id_tiempo IS NULL
   OR id_sucursal IS NULL
   OR id_empleado IS NULL
   OR id_canal IS NULL
   OR id_metodo_pago IS NULL;
   
-- =========================================
--  3. Validaciones de consistencia de datos
-- Productos sin categoría o marca (afecta filtros en Power BI)
SELECT COUNT(*) FROM dw.dim_producto 
WHERE categoria IS NULL OR categoria = 'Sin Categoría';
--Buscar subtotales negativos o en cero
SELECT * 
FROM dw.fact_ventas 
WHERE subtotal <= 0;

-- =========================================

-- 4. Validaciones de métrica
--cantidades validas
SELECT * FROM dw.fact_ventas WHERE cantidad <= 0;
--precios validos
SELECT * FROM dw.fact_ventas WHERE precio_unitario_cobrado < 0 OR costo_historico < 0;
--subtotal coherente
SELECT * FROM dw.fact_ventas WHERE subtotal <> cantidad * precio_unitario_cobrado;
--costo logico
SELECT * FROM dw.fact_ventas WHERE costo_historico > precio_unitario_cobrado;
--valores extremos
SELECT * FROM dw.fact_ventas WHERE cantidad > 100;
-- =========================================

-- 5. Validaciones temporales
SELECT * FROM dw.fact_ventas f LEFT JOIN dw.dim_tiempo t ON f.id_tiempo = t.id_tiempo
WHERE t.fecha IS NULL;

SELECT * FROM dw.fact_ventas f JOIN dw.dim_tiempo t ON f.id_tiempo = t.id_tiempo
WHERE t.fecha > CURRENT_DATE;
-- promo fuera de rango
SELECT * FROM dw.fact_ventas f JOIN dw.dim_tiempo t ON f.id_tiempo = t.id_tiempo
JOIN dw.dim_promocion p ON f.id_promocion = p.id_promocion
WHERE t.fecha NOT BETWEEN p.fecha_inicio AND p.fecha_fin;
-- =========================================

--6. Validaciones de completitud
--lineas de ventas
SELECT
    (SELECT COUNT(*) FROM public.ventas_detalle) AS lineas_oltp,
    (SELECT COUNT(*) FROM dw.fact_ventas) AS lineas_dw;

--totales
SELECT
    (SELECT SUM(subtotal) FROM public.ventas_detalle) AS total_oltp,
    (SELECT SUM(subtotal) FROM dw.fact_ventas) AS total_dw;
--cobertura de tickets
SELECT vc.id FROM public.ventas_cabecera vc LEFT JOIN public.ventas_detalle vd 
ON vc.id = vd.venta_id
WHERE vd.id IS NULL;
--Todas las ventas en ventas_cabecera tienen al menos una linea en ventas_detalle

-- =========================================

-- 7. Validaciones de duplicados
--tickets duplicados
SELECT nro_ticket, COUNT(*) FROM dw.fact_ventas GROUP BY nro_ticket
HAVING COUNT(*) > 1;
--producto duplicados por sku
SELECT sku, COUNT(*) FROM dw.dim_producto GROUP BY sku
HAVING COUNT(*) > 1;
-- 8. Consulta de métricas de control en el Data Warehouse (Esquema DW)
SELECT 
    count(*) AS total_fact,
    sum(cantidad) AS unidades_vendidas,
    sum(subtotal) AS facturacion_total
FROM dw.fact_ventas;

-- 9. Consultas de control en el origen (Esquema Public / OLTP)
SELECT count(*) AS total_origen FROM public.ventas_detalle;
SELECT sum(cantidad) AS unidades_origen FROM public.ventas_detalle;
SELECT sum(subtotal) AS facturacion_origen FROM public.ventas_detalle;
