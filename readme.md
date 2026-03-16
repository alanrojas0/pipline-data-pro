# 🚀 Professional Data Pipeline (Medallion Architecture)

Este proyecto implementa un flujo de **Ingeniería de Datos** completo y automatizado, utilizando estándares de la industria para asegurar la escalabilidad y portabilidad de los datos.

## 🏗️ Arquitectura de Medallón
El pipeline organiza los datos en tres capas lógicas:
- **Bronze:** Ingesta de datos crudos (Raw Data).
- **Silver:** Limpieza, filtrado de valores nulos/negativos y tipado de datos.
- **Gold:** Agregaciones de negocio listas para consumo analítico (BI).

## 🛠️ Tecnologías Utilizadas
- **Python (Pandas):** Motor de transformación de datos.
- **Docker:** Contenerización para asegurar que el pipeline corra en cualquier entorno.
- **GitHub Actions (CI/CD):** Automatización total del flujo tras cada actualización de código.

## ⚙️ Cómo se ejecuta
El proyecto está configurado para ejecutarse automáticamente mediante **GitHub Actions**. 
Cada vez que se realiza un `push` a la rama principal, un contenedor Docker se levanta en la nube de GitHub, construye la imagen y ejecuta el proceso ETL completo.

---
*Proyecto desarrollado para demostrar competencias en ingeniería de datos, automatización y despliegue en la nube.*