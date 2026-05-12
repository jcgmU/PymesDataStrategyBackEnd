import jwt from 'jsonwebtoken';

const token = jwt.sign(
  { id: '12345678-1234-1234-1234-1234567890ab', email: 'test@example.com', role: 'USER' },
  'dev-jwt-secret-change-in-production',
  { expiresIn: '1d' }
);
console.log(token);
