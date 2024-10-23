import Keycloak from 'keycloak-js';

const keycloak = new Keycloak({
  url: 'https://zambom.botcity.dev:8050/',
  realm: 'teste',
  clientId: 'teste',
});

export default keycloak;